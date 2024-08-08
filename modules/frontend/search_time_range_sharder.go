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

// todo: jpe
//  blocks have to be decided in one spot - time range sharder and passed to the search sharder
//  accept perf regression on creating a bunch of goroutines. fix later by fixing the lock in the buried queue. the only reason we have so many goroutines is to put pressure on that lock

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
