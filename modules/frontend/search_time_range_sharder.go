package frontend

import (
	"fmt"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
)

// sharder that takes an interface? or functions to provide a generic way to shard a given query over
// a time range. we will execute queries in reverse time order starting from the end and progressing
// toward the start. every time a query finishes we will check the combiner to see if the query is
// complete. if it is then we will sort and return the results

// add a local interface for a "combinersorter" to sort results

// todo: jpe
//  - execute one request for each hour working backwards
//    - 2 simultaneous requests
//  - combiner no longer quits early if limit is reached
//  - after each batch check add all traces to a mega combiner and check if the limit is reached
//  - adjust the sharder to be exclusive on either the front or end of the range
//  - time range sharder passes a new request struct, url is original request range. shard start/end are added to the struct and are used for block choices
//  - handle start/end = 0 i.e. an ingester only search

// JPE!?
//  . search metrics are broken? 24 jobs for 24 hours of search? - seemed fine in tempo-dev-01
// . send all data back immediately
//   . send back a marker occassiaonlly saying THIS MUCH DONE. only send that much back
// . add a "metadata response"

// . completed jobs is often 1 less of total jobs
// . find a way to pass actual total jobs from the time range sharder. bar repeatedly resets in grafana

// JPE TODAY:
// shardedSearchRequest is an already parsed http request used by search_sharder.go
//  includes a slice of block metas to search this allows the time range sharder to
//  correctly calculate the total number of jobs to send back to the combiner
//  AND avoids bugs where the blocklist changes in between shards received
// 1) pull block metas in a local var from the .reader with a new filter func
// 3) don't break on start/end time shards. just send 100 at a time or something simple.
//   4) subslice the block metas instead of alloc'ing new

// jpe
//  start a cross tenant limitations doc? include the fact that multitenant is not guaranteed to return the most recent traces

const shardDuration = 3600

type shardedSearchRequest struct {
	pipeline.Request

	parsedRequest    *tempopb.SearchRequest
	blocks           []*backend.BlockMeta
	ingesterRequests bool
}

// jpe - not technically necessary anymore?
func (s *shardedSearchRequest) Clone() pipeline.Request {
	return &shardedSearchRequest{
		Request:          s.Request.Clone(),
		parsedRequest:    s.parsedRequest,
		blocks:           s.blocks,
		ingesterRequests: s.ingesterRequests,
	}
}

type asyncTimeRangeSearchSharder struct {
	next      pipeline.AsyncRoundTripper[combiner.PipelineResponse]
	reader    tempodb.Reader
	cfg       SearchSharderConfig
	overrides overrides.Interface
}

// newTimeRangeSearchSharder creates 1 hour time ranges working backwards from the end of the range
func newAsyncTimeRangeSearchSharder(reader tempodb.Reader, o overrides.Interface, cfg SearchSharderConfig) pipeline.AsyncMiddleware[combiner.PipelineResponse] {
	return pipeline.AsyncMiddlewareFunc[combiner.PipelineResponse](func(next pipeline.AsyncRoundTripper[combiner.PipelineResponse]) pipeline.AsyncRoundTripper[combiner.PipelineResponse] {
		return asyncTimeRangeSearchSharder{
			next:      next,
			reader:    reader,
			cfg:       cfg,
			overrides: o,
		}
	})
}

func (s asyncTimeRangeSearchSharder) RoundTrip(pipelineRequest pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
	// jpe - defer cancel context? technically the collector above will ...
	//       handle failed context?
	r := pipelineRequest.HTTPRequest()
	ctx := pipelineRequest.Context()

	tenantID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}

	searchReq, err := api.ParseSearchRequest(r)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}

	// adjust limit based on config
	searchReq.Limit, err = adjustLimit(searchReq.Limit, s.cfg.DefaultLimit, s.cfg.MaxLimit)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}

	// calculate and enforce max search duration
	maxDuration := s.maxDuration(tenantID)
	if maxDuration != 0 && time.Duration(searchReq.End-searchReq.Start)*time.Second > maxDuration {
		return pipeline.NewBadRequest(fmt.Errorf("range specified by start and end exceeds %s. received start=%d end=%d", maxDuration, searchReq.Start, searchReq.End)), nil
	}

	// if the start and end are 0 then we are searching ingesters only and the time sharding doesn't matter,
	// just pass to the next middleware
	if searchReq.End == 0 && searchReq.Start == 0 {
		return s.next.RoundTrip(&shardedSearchRequest{ // jpe to pointer or not to pointer
			Request:          pipelineRequest,
			parsedRequest:    searchReq,
			blocks:           nil,
			ingesterRequests: true,
		})
	}

	// metas are returned in reverse order by end time
	metas := s.reader.BlockMetas(tenantID)
	metas = s.blockMetas(metas, searchReq.Start, searchReq.End)

	// jpe calc and send total jobs, etc off of the block metas

	asyncResponseSender := pipeline.NewAsyncResponseSender()

	go func() {
		defer asyncResponseSender.SendComplete()

		ingesterRequests := true // jpe - not necessarily. needs to be based on whether the time range overlaps the ingester range

		for len(metas) > 0 {
			blocksPerShard := 10 // jpe - const
			if len(metas) < blocksPerShard {
				blocksPerShard = len(metas)
			}
			shardMetas := metas[:blocksPerShard]

			resps, err := s.next.RoundTrip(&shardedSearchRequest{ // jpe to pointer or not to pointer
				Request:          pipelineRequest,
				parsedRequest:    searchReq,
				blocks:           shardMetas,
				ingesterRequests: ingesterRequests,
			})
			if err != nil {
				asyncResponseSender.SendError(err)
				return
			}

			ingesterRequests = false

			for {
				resp, done, err := resps.Next(r.Context())
				if err != nil {
					asyncResponseSender.SendError(err)
					return
				}

				if resp == nil {
					break
				}

				asyncResponseSender.Send(r.Context(), pipeline.NewAsyncResponse(resp)) // jpe - allocs? should asyncresponder take a combiner.PipelineResponse directly?

				if done {
					break
				}
			}

			// we've now sent to the combiner all of the potential results from now through the end of the final block we searched
			// send that end time to the combiner so it can track progress
			end := uint32(shardMetas[0].EndTime.Unix())

			asyncResponseSender.Send(r.Context(), pipeline.NewAsyncResponse(&combiner.ShardCompletionResponse{
				CompletedThrough: end,
			}))
		}
	}()

	return asyncResponseSender, nil
}

// blockMetas returns all relevant blockMetas given a start/end
func (s *asyncTimeRangeSearchSharder) blockMetas(metas []*backend.BlockMeta, start, end uint32) []*backend.BlockMeta {
	// subslice metas to those in the requested range
	retMetas := make([]*backend.BlockMeta, 0, len(metas)/100) // 50 for luck

	// reduce metas to those in the requested range.
	for _, m := range metas {
		if m.ReplicationFactor != backend.DefaultReplicationFactor { // This check skips generator blocks (RF=1)
			continue
		}

		// blocks completely outside the bounds
		blockStart := uint32(m.StartTime.Unix())
		blockEnd := uint32(m.EndTime.Unix())
		if blockStart > end {
			continue
		}
		if blockEnd < start {
			continue
		}

		retMetas = append(retMetas, m)
	}

	return retMetas
}

// maxDuration returns the max search duration allowed for this tenant.
func (s *asyncTimeRangeSearchSharder) maxDuration(tenantID string) time.Duration {
	// check overrides first, if no overrides then grab from our config
	maxDuration := s.overrides.MaxSearchDuration(tenantID)
	if maxDuration != 0 {
		return maxDuration
	}

	return s.cfg.MaxDuration
}
