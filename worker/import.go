/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/hypermodeinc/dgraph/v25/conn"
	"github.com/hypermodeinc/dgraph/v25/posting"
	"github.com/hypermodeinc/dgraph/v25/protos/pb"
	"github.com/hypermodeinc/dgraph/v25/schema"
	"github.com/hypermodeinc/dgraph/v25/x"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type pubSub struct {
	sync.RWMutex
	subscribers []chan *api.StreamExtSnapshotRequest
}

// Subscribe returns a new channel to receive published messages
func (ps *pubSub) subscribe() chan *api.StreamExtSnapshotRequest {
	ch := make(chan *api.StreamExtSnapshotRequest, 20)
	ps.Lock()
	ps.subscribers = append(ps.subscribers, ch)
	ps.Unlock()
	return ch
}

// publish sends to all subscribers without blocking on any single subscriber.
// If a subscriber is not draining, skip it (it will be marked failed by its goroutine).
func (ps *pubSub) publish(msg *api.StreamExtSnapshotRequest) {
	ps.RLock()
	// copy to avoid holding lock while sending
	subs := make([]chan *api.StreamExtSnapshotRequest, len(ps.subscribers))
	copy(subs, ps.subscribers)
	ps.RUnlock()

	for _, ch := range subs {
		select {
		case ch <- msg:
		default:
			// subscriber is stuck; skip to avoid blocking publisher
		}
	}
}

// Unsubscribe removes a subscriber and closes its channel.
func (ps *pubSub) unsubscribe(ch chan *api.StreamExtSnapshotRequest) {
	ps.Lock()
	defer ps.Unlock()
	for i, c := range ps.subscribers {
		if c == ch {
			// remove from slice
			ps.subscribers = append(ps.subscribers[:i], ps.subscribers[i+1:]...)
			close(c)
			return
		}
	}
}

func (ps *pubSub) close() {
	ps.Lock()
	defer ps.Unlock()
	for _, ch := range ps.subscribers {
		close(ch)
	}
	ps.subscribers = nil
}

func (ps *pubSub) handlePublisher(ctx context.Context, stream api.Dgraph_StreamExtSnapshotServer) error {
	for {
		select {
		case <-ctx.Done():
			glog.Infof("[import] Context cancelled, stopping receive goroutine: %v", ctx.Err())
			return ctx.Err()

		default:
			msg, err := stream.Recv()
			if err != nil && !errors.Is(err, io.EOF) {
				glog.Errorf("[import] error receiving from in stream: %v", err)
				return err
			} else if err == io.EOF {
				return nil
			}
			if msg == nil {
				continue
			}
			ps.publish(msg)

			if msg.Pkt != nil && msg.Pkt.Done {
				glog.Infof("[import] Received Done signal, breaking the loop")
				return nil
			}
			if err := stream.Send(&api.StreamExtSnapshotResponse{Finish: false}); err != nil {
				return err
			}
		}
	}
}

func (ps *pubSub) runForwardSubscriber(ctx context.Context, out api.Dgraph_StreamExtSnapshotClient, peerId string) error {
	defer func() {
		glog.Infof("[import] forward subscriber stopped for peer [%v]", peerId)
	}()

	buffer := ps.subscribe()
	defer ps.unsubscribe(buffer) // ensure publisher won't block on us if we exit

Loop:
	for {
		select {
		case <-ctx.Done():
			glog.Infof("[import] Context cancelled, stopping receive goroutine: %v", ctx.Err())
			return ctx.Err()

		default:
			msg, ok := <-buffer
			if !ok {
				break Loop
			}

			if msg.Pkt.Done {
				glog.Infof("[import] received done signal from [%v]", peerId)
				d := api.StreamPacket{Done: true}
				if err := out.Send(&api.StreamExtSnapshotRequest{Pkt: &d}); err != nil {
					return err
				}

				_ = out.CloseSend()

				for {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					r, err := out.Recv()
					if errors.Is(err, io.EOF) {
						return fmt.Errorf("server closed stream before Finish=true for peer [%v]", peerId)
					}
					if err != nil {
						return fmt.Errorf("failed to receive final response from peer [%v]: %w", peerId, err)
					}
					if r.Finish {
						glog.Infof("[import] peer [%v]: Received final Finish=true", peerId)
						break Loop
					}
					glog.Infof("[import] peer [%v]: Waiting for Finish=true, got interim ACK", peerId)
				}
			}

			data := &api.StreamExtSnapshotRequest{Pkt: &api.StreamPacket{Data: msg.Pkt.Data}}
			if err := out.Send(data); err != nil {
				return err
			}

			if _, err := out.Recv(); err != nil {
				return fmt.Errorf("failed to receive response from peer [%v]: %w", peerId, err)
			}
		}
	}

	return nil
}

func (ps *pubSub) runLocalSubscriber(ctx context.Context, stream pb.Worker_StreamExtSnapshotServer) error {
	defer func() {
		glog.Infof("[import] local subscriber stopped")
	}()

	buffer := ps.subscribe()
	defer ps.unsubscribe(buffer) // ensure publisher won't block on us if we exit
	glog.Infof("[import:flush] flushing external snapshot in badger db")

	sw := pstore.NewStreamWriter()
	defer sw.Cancel()
	if err := sw.Prepare(); err != nil {
		return err
	}

Loop:
	for {
		select {
		case <-ctx.Done():
			glog.Infof("[import] Context cancelled, stopping receive goroutine: %v", ctx.Err())
			return ctx.Err()

		default:
			msg, ok := <-buffer
			if !ok {
				break Loop
			}
			kvs := msg.GetPkt()
			if kvs == nil {
				continue
			}
			if kvs.Done {
				break
			}

			buf := z.NewBufferSlice(kvs.Data)
			if err := sw.Write(buf); err != nil {
				return err
			}
		}
	}

	if err := sw.Flush(); err != nil {
		return err
	}

	glog.Infof("[import:flush] successfully flushed data in badger db")
	if err := postStreamProcessing(ctx); err != nil {
		return err
	}
	return nil
}

func ProposeDrain(ctx context.Context, drainMode *api.UpdateExtSnapshotStreamingStateRequest) ([]uint32, error) {
	members := GetMembershipState()
	currentGroups := make([]uint32, 0)
	for gid := range members.GetGroups() {
		currentGroups = append(currentGroups, gid)
	}

	for _, gid := range currentGroups {
		if groups().ServesGroup(gid) && groups().Node.AmLeader() {
			if _, err := (&grpcWorker{}).UpdateExtSnapshotStreamingState(ctx, drainMode); err != nil {
				return nil, err
			}
			continue
		}
		glog.Infof("[import:apply-drainmode] Connecting to the leader of the group [%v] from alpha addr [%v]",
			gid, groups().Node.MyAddr)

		pl := groups().Leader(gid)
		if pl == nil {
			glog.Errorf("[import:apply-drainmode] unable to connect to the leader of group [%v]", gid)
			return nil, fmt.Errorf("unable to connect to the leader of group [%v] : %v", gid, conn.ErrNoConnection)
		}

		c := pb.NewWorkerClient(pl.Get())
		glog.Infof("[import:apply-drainmode] Successfully connected to leader of group [%v]", gid)

		if _, err := c.UpdateExtSnapshotStreamingState(ctx, drainMode); err != nil {
			glog.Errorf("[import:apply-drainmode] unable to apply drainmode : %v", err)
			return nil, err
		}
	}

	return currentGroups, nil
}

// InStream handles streaming of snapshots to a target group. It first checks the group
// associated with the incoming stream and, if it's the same as the current node's group, it
// flushes the data using FlushKvs. If the group is different, it establishes a connection
// with the leader of that group and streams data to it. The function returns an error if
// there are any issues in the process, such as a broken connection or failure to establish
// a stream with the leader.
func InStream(stream api.Dgraph_StreamExtSnapshotServer) error {
	req, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive initial stream message: %v", err)
	}

	if err := stream.Send(&api.StreamExtSnapshotResponse{Finish: false}); err != nil {
		return fmt.Errorf("failed to send initial response: %v", err)
	}

	groupId := req.GroupId
	if groupId == groups().Node.gid {
		glog.Infof("[import] streaming external snapshot to current group [%v]", groupId)
		return streamInGroup(stream, true)
	}

	glog.Infof("[import] streaming external snapshot to other group [%v]", groupId)
	pl := groups().Leader(groupId)
	if pl == nil {
		glog.Errorf("[import] unable to connect to the leader of group [%v]", groupId)
		return fmt.Errorf("unable to connect to the leader of group [%v] : %v", groupId, conn.ErrNoConnection)
	}

	con := pl.Get()
	c := pb.NewWorkerClient(con)
	alphaStream, err := c.StreamExtSnapshot(stream.Context())
	if err != nil {
		glog.Errorf("[import] failed to establish stream with leader: %v", err)
		return fmt.Errorf("failed to establish stream with leader: %v", err)
	}
	glog.Infof("[import] [forward %d -> %d] start", groups().Node.gid, groupId)
	glog.Infof("[import] [forward %d -> %d] start", groups().Node.MyAddr, groups().Leader(groupId).Addr)

	glog.Infof("[import] sending forward true to leader of group [%v]", groupId)
	forwardReq := &api.StreamExtSnapshotRequest{Forward: true}
	if err := alphaStream.Send(forwardReq); err != nil {
		glog.Errorf("[import] failed to send forward request: %v", err)
		return fmt.Errorf("failed to send forward request: %v", err)
	}

	return pipeTwoStream(stream, alphaStream, groupId)
}

func pipeTwoStream(in api.Dgraph_StreamExtSnapshotServer, out pb.Worker_StreamExtSnapshotClient, groupId uint32) error {
	currentGroup := groups().Node.gid
	ctx := in.Context()
	if err := out.Send(&api.StreamExtSnapshotRequest{GroupId: groupId}); err != nil {
		return fmt.Errorf("send groupId downstream(%d): %w", groupId, err)
	}
	if _, err := out.Recv(); err != nil {
		return fmt.Errorf("ack groupId downstream(%d): %w", groupId, err)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		req, err := in.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("recv upstream(%d): %w", currentGroup, err)
		}
		if req.Pkt == nil {
			return fmt.Errorf("unexpected empty request")
		}

		if req.Pkt.Done {
			// Forward Done, half-close downstream send.
			if err := out.Send(&api.StreamExtSnapshotRequest{Pkt: req.Pkt}); err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("send done downstream(%d): %w", groupId, err)
			}
			_ = out.CloseSend()

			// Drain downstream and relay upstream until Finish=true.
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				resp, err := out.Recv()
				if errors.Is(err, io.EOF) {
					return fmt.Errorf("downstream(%d) closed before Finish=true", groupId)
				}
				if err != nil {
					return fmt.Errorf("recv final downstream(%d): %w", groupId, err)
				}
				if err := in.Send(resp); err != nil {
					return fmt.Errorf("relay final upstream: %w", err)
				}
				if resp.Finish {
					glog.Infof("[import] [forward %d -> %d] finish", currentGroup, groupId)
					return nil
				}
			}
		}

		// Normal data chunk: send -> wait ack -> send upstream ack.
		if err := out.Send(&api.StreamExtSnapshotRequest{Pkt: req.Pkt}); err != nil {
			return fmt.Errorf("send data downstream(%d): %w", groupId, err)
		}
		if _, err := out.Recv(); err != nil {
			return fmt.Errorf("ack data downstream(%d): %w", groupId, err)
		}
		if err := in.Send(&api.StreamExtSnapshotResponse{}); err != nil {
			return fmt.Errorf("send ack upstream: %w", err)
		}

	}
}

func (w *grpcWorker) UpdateExtSnapshotStreamingState(ctx context.Context,
	req *api.UpdateExtSnapshotStreamingStateRequest) (*pb.Status, error) {
	if req == nil {
		return nil, errors.New("UpdateExtSnapshotStreamingStateRequest must not be nil")
	}

	if req.Start && req.Finish {
		return nil, errors.New("UpdateExtSnapshotStreamingStateRequest cannot have both Start and Finish set to true")
	}

	glog.Infof("[import] Applying import mode proposal: %+v", req)
	err := groups().Node.proposeAndWait(ctx, &pb.Proposal{ExtSnapshotState: req})

	return &pb.Status{}, err
}

// StreamExtSnapshot handles the stream of key-value pairs sent from proxy alpha.
// It receives a Forward flag from the stream to determine if the current node is the leader.
// If the node is the leader (Forward is true), it streams the data to its followers.
// Otherwise, it simply writes the data to BadgerDB and flushes it.
func (w *grpcWorker) StreamExtSnapshot(stream pb.Worker_StreamExtSnapshotServer) error {
	glog.Info("[import] trying to update the import mode to false")
	defer x.ExtSnapshotStreamingState(false)

	// Receive the first message to check the Forward flag.
	// If Forward is true, this node is the leader and should forward the stream to its followers.
	// If Forward is false, the node just writes and flushes the data.
	forwardReq, err := stream.Recv()
	if err != nil {
		return err
	}

	glog.Infof("[import] received forward flag: %v", forwardReq.Forward)
	return streamInGroup(stream, forwardReq.Forward)
}

// postStreamProcessing handles the post-stream processing of data received from the buffer into the local BadgerDB.
// It loads the schema, updates the membership state, informs zero about tablets, resets caches, applies initial schema,
// applies initial types, and resets the GQL schema store.
func postStreamProcessing(ctx context.Context) error {
	glog.Info("[import:flush] post stream processing")
	if err := schema.LoadFromDb(ctx); err != nil {
		return errors.Wrapf(err, "cannot load schema after streaming data")
	}
	if err := UpdateMembershipState(ctx); err != nil {
		return errors.Wrapf(err, "cannot update membership state after streaming data")
	}

	gr.informZeroAboutTablets()
	posting.ResetCache()
	ResetAclCache()
	groups().applyInitialSchema()
	groups().applyInitialTypes()
	ResetGQLSchemaStore()
	glog.Info("[import:flush] post stream processing done")
	return nil
}

// streamInGroup handles the streaming of data within a group.
// This function is called on both leader and follower nodes with different behaviors:
// - Leader (forward=true): The leader node receives data and forwards it to all group members
// - Follower (forward=false): The follower node receives data and stores it in local BadgerDB.
//
// Parameters:
// - stream: The gRPC stream server for receiving streaming data
// - forward: Indicates if this node is forwarding data to other nodes
//   - true: This node is the group leader and will forward data to group members
//   - false: This node is a follower receiving forwarded data and storing locally
//
// The function:
// 1. Creates a context with cancellation support for graceful shutdown
// 2. Sets up a pub/sub system for message distribution
// 3. Uses an error group to manage concurrent operations
// 4. Tracks successful nodes for majority consensus (only relevant for leader)
// 5. Receives messages in a loop until EOF or error
// 6. Publishes received messages to all subscribers (for leader) or stores locally (for follower)
// 7. Handles cleanup and error cases appropriately
//
// Returns:
// - nil: If streaming completes successfully
// - error: If there's an issue receiving data or if majority consensus isn't achieved (for leader)
func streamInGroup(stream api.Dgraph_StreamExtSnapshotServer, forward bool) error {
	node := groups().Node
	glog.Infof("[import] got stream, forwarding in group [%v]", forward)

	// We created this to check the majority
	successfulNodes := make(map[string]bool)

	ps := &pubSub{}
	eg, errGCtx := errgroup.WithContext(stream.Context())
	for _, member := range groups().state.Groups[node.gid].Members {
		if member.Addr == node.MyAddr {
			eg.Go(func() error {
				if err := ps.runLocalSubscriber(errGCtx, stream); err != nil {
					glog.Errorf("[import:flush] failed to run local subscriber: %v", err)
					updateNodeStatus(&ps.RWMutex, successfulNodes, member.Addr, false)
					return err
				}

				updateNodeStatus(&ps.RWMutex, successfulNodes, member.Addr, true)
				return nil
			})
			continue
		}

		if forward {
			// We are not going to return any error from here because we care about the majority of nodes.
			// If the majority of nodes are able to receive the data, the remaining ones can catch up later.
			glog.Infof("[import] Streaming external snapshot to [%v] from [%v] forward [%v]", member.Addr, node.MyAddr)
			eg.Go(func() error {
				glog.Infof(`[import:forward] streaming external snapshot to [%v] from [%v]`, member.Addr, node.MyAddr)
				if member.AmDead {
					glog.Infof(`[import:forward] [%v] is dead, skipping`, member.Addr)
					return nil
				}

				pl, err := conn.GetPools().Get(member.Addr)
				if err != nil {
					updateNodeStatus(&ps.RWMutex, successfulNodes, member.Addr, false)
					glog.Errorf("connection error to [%v]: %v", member.Addr, err)
					return nil
				}

				c := pb.NewWorkerClient(pl.Get())
				peerStream, err := c.StreamExtSnapshot(errGCtx)
				if err != nil {
					updateNodeStatus(&ps.RWMutex, successfulNodes, member.Addr, false)
					glog.Errorf("failed to establish stream with peer %v: %v", member.Addr, err)
					return nil
				}
				defer func() {
					if err := peerStream.CloseSend(); err != nil {
						glog.Errorf("[import:forward] failed to close stream with peer [%v]: %v", member.Addr, err)
					}
				}()

				forwardReq := &api.StreamExtSnapshotRequest{Forward: false}
				if err := peerStream.Send(forwardReq); err != nil {
					updateNodeStatus(&ps.RWMutex, successfulNodes, member.Addr, false)
					glog.Errorf("failed to send forward request: %v", err)
					return nil
				}

				if err := ps.runForwardSubscriber(errGCtx, peerStream, member.Addr); err != nil {
					updateNodeStatus(&ps.RWMutex, successfulNodes, member.Addr, false)
					glog.Errorf("failed to run forward subscriber: %v", err)
					return nil
				}

				updateNodeStatus(&ps.RWMutex, successfulNodes, member.Addr, true)
				glog.Infof("[import] Successfully connected and streamed data to node: %v", member.Addr)
				return nil
			})
		}
	}

	eg.Go(func() error {
		defer ps.close()
		defer func() {
			if err := stream.Send(&api.StreamExtSnapshotResponse{}); err != nil {
				glog.Errorf("[import] failed to send close on in: %v", err)
			}
		}()
		if err := ps.handlePublisher(errGCtx, stream); err != nil {
			return fmt.Errorf("failed to run publisher: %v", err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to run in group streaming: %v", err)
	}
	// Sends a StreamExtSnapshotResponse with the Finish flag set to true to indicate
	// the completion of the streaming process. If an error occurs during the send
	// operation, it is returned for further handling.
	if err := stream.Send(&api.StreamExtSnapshotResponse{Finish: true}); err != nil {
		glog.Errorf("failed to send done signal: %v", err)
	}

	// If this node is the leader and fails to reach a majority of nodes, we return an error.
	// This ensures that the data is reliably received by enough nodes before proceeding.
	if forward && !checkMajority(successfulNodes) {
		glog.Error("[import] Majority of nodes failed to receive data.")
		return fmt.Errorf("failed to send data to majority of the nodes")
	}
	return nil
}

func updateNodeStatus(ps *sync.RWMutex, successfulNodes map[string]bool, addr string, status bool) {
	ps.Lock()
	successfulNodes[addr] = status
	ps.Unlock()
}

// Calculate majority based on Raft quorum rules with special handling for small clusters
func checkMajority(successfulNodes map[string]bool) bool {
	totalNodes := len(successfulNodes)
	successfulCount := 0

	for _, success := range successfulNodes {
		if success {
			successfulCount++
		}
	}

	// Special cases for small clusters
	switch totalNodes {
	case 0:
		// No nodes - this should never happen
		glog.Error("[import] No nodes in cluster")
		return false
	case 1:
		// Single node - must succeed
		return successfulCount == 1
	case 2:
		// Two nodes - both must succeed
		return successfulCount == 2
	default:
		// Regular Raft quorum rule for 3+ nodes
		majority := totalNodes/2 + 1
		return successfulCount >= majority
	}
}
