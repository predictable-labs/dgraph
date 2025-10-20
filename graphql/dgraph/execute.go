/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package dgraph

import (
	"context"
	"strings"

	"github.com/golang/glog"
	"go.opencensus.io/trace"

	dgoapi "github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/hypermodeinc/dgraph/v25/edgraph"
	"github.com/hypermodeinc/dgraph/v25/graphql/schema"
	"github.com/hypermodeinc/dgraph/v25/x"
)

// DgraphEx is the executor that bridges the GraphQL layer with the Dgraph storage layer.
// It's used by GraphQL resolvers to execute queries and mutations against Dgraph.
type DgraphEx struct{}

// Execute is the bridge between GraphQL resolvers and the Dgraph query engine.
// This is where rewritten DQL queries (from GraphQL) are sent to Dgraph for execution.
//
// Parameters:
//   - ctx: Context with authentication and namespace information
//   - req: DQL query/mutation request (already rewritten from GraphQL)
//   - field: GraphQL field being resolved (used for special formatting, nil for DQL queries)
//
// Returns:
//   - Response with JSON data (in GraphQL format if field is non-nil, DQL format otherwise)
//
// Flow:
// 1. Validate request is not empty
// 2. Mark request as coming from GraphQL layer
// 3. Execute through edgraph.Server.QueryGraphQL()
// 4. Return response with proper error wrapping
func (dg *DgraphEx) Execute(ctx context.Context, req *dgoapi.Request,
	field schema.Field) (*dgoapi.Response, error) {

	// === PHASE 1: Tracing Setup ===
	// Add span timing for performance monitoring
	span := trace.FromContext(ctx)
	stop := x.SpanTimer(span, "dgraph.Execute")
	defer stop()

	// === PHASE 2: Empty Request Check ===
	// Return early if there's nothing to execute
	if req == nil || (req.Query == "" && len(req.Mutations) == 0) {
		return nil, nil
	}

	// === PHASE 3: Debug Logging ===
	// Log the DQL query and mutations being executed
	if glog.V(3) {
		muts := make([]string, len(req.Mutations))
		for i, m := range req.Mutations {
			muts[i] = m.String()
		}

		glog.Infof("Executing Dgraph request; with\nQuery: \n%s\nMutations:%s",
			req.Query, strings.Join(muts, "\n"))
	}

	// === PHASE 4: Mark as GraphQL Request ===
	// Set context flag so edgraph layer knows this came from GraphQL
	// This affects validation, authorization, and error handling
	ctx = context.WithValue(ctx, edgraph.IsGraphql, true)

	// === PHASE 5: Execute Query/Mutation ===
	// Hand off to edgraph.Server.QueryGraphQL() which will:
	// - Route to doQuery() in edgraph/server.go
	// - Execute query through query engine (query.Request.Process)
	// - Execute mutations if present
	// - Format response as JSON
	resp, err := (&edgraph.Server{}).QueryGraphQL(ctx, req, field)
	if !x.IsGqlErrorList(err) {
		err = schema.GQLWrapf(err, "Dgraph execution failed")
	}

	return resp, err
}

// CommitOrAbort commits or aborts a Dgraph transaction.
// Used by mutation resolvers to finalize transactions after mutations complete.
//
// Parameters:
//   - ctx: Request context
//   - tc: Transaction context with StartTs and commit/abort flag
//
// Returns:
//   - Updated transaction context with CommitTs if committed
func (dg *DgraphEx) CommitOrAbort(ctx context.Context,
	tc *dgoapi.TxnContext) (*dgoapi.TxnContext, error) {
	return (&edgraph.Server{}).CommitOrAbort(ctx, tc)
}
