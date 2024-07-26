package frontend

import (
	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/pkg/api"
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
// 1) pull block metas from the .reader with a new filter func
// 2) sort them by end time? and send subslices to the next middleware?
//    2a) already sorted by time. write test to pin this behavior and rely on it here
// 3) don't break on start/end time shards. just send 100 at a time or something simple.

// jpe
//  start a cross tenant limitations doc? include the fact that multitenant is not guaranteed to return the most recent traces

const shardDuration = 3600

type shardedSearchRequest struct {
	pipeline.Request

	shardStart uint32
	shardEnd   uint32
}

func (s *shardedSearchRequest) Clone() pipeline.Request {
	return &shardedSearchRequest{
		Request:    s.Request.Clone(),
		shardStart: s.shardStart,
		shardEnd:   s.shardEnd,
	}
}

type asyncTimeRangeSearchSharder struct {
	next pipeline.AsyncRoundTripper[combiner.PipelineResponse]
}

// newTimeRangeSearchSharder creates 1 hour time ranges working backwards from the end of the range
func newAsyncTimeRangeSearchSharder() pipeline.AsyncMiddleware[combiner.PipelineResponse] {
	return pipeline.AsyncMiddlewareFunc[combiner.PipelineResponse](func(next pipeline.AsyncRoundTripper[combiner.PipelineResponse]) pipeline.AsyncRoundTripper[combiner.PipelineResponse] {
		return asyncTimeRangeSearchSharder{
			next: next,
		}
	})
}

func (s asyncTimeRangeSearchSharder) RoundTrip(pipelineRequest pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
	// jpe - defer cancel context? technically the collector above will ...
	//       handle failed context?
	r := pipelineRequest.HTTPRequest()

	searchReq, err := api.ParseSearchRequest(r)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}

	// if the start and end are 0 then we are searching ingesters only and the time sharding doesn't matter,
	// just pass to the next middleware
	shards := shardTotal(searchReq.Start, searchReq.End)
	if shards == 0 {
		return s.next.RoundTrip(&shardedSearchRequest{ // jpe to pointer or not to pointer
			Request:    pipelineRequest,
			shardStart: 0,
			shardEnd:   0,
		})
	}

	asyncResponseSender := pipeline.NewAsyncResponseSender()

	go func() {
		defer asyncResponseSender.SendComplete()

		for i := 0; i < shards; i++ {
			start, end := shardStartEnd(i, searchReq.Start, searchReq.End)

			resps, err := s.next.RoundTrip(&shardedSearchRequest{ // jpe to pointer or not to pointer
				Request:    pipelineRequest,
				shardStart: start,
				shardEnd:   end,
			})
			if err != nil {
				asyncResponseSender.SendError(err)
				return
			}

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

			// send a completion marker to the combiner
			asyncResponseSender.Send(r.Context(), pipeline.NewAsyncResponse(&combiner.ShardCompletionResponse{
				CompletedThrough: end, // jpe - start?
			}))
		}
	}()

	return asyncResponseSender, nil
}

// shardTotal returns the number of shards between start and end
func shardTotal(start, end uint32) int {
	if start >= end {
		return 0
	}

	totalRange := end - start
	shards := int(totalRange / shardDuration)

	if totalRange%shardDuration != 0 {
		shards++
	}
	return shards
}

// jpe - shard end matches previous shard start, does this need to be adjusted?
func shardStartEnd(shard int, rangeStart, rangeEnd uint32) (uint32, uint32) {
	// invalid range
	if rangeStart >= rangeEnd {
		return 0, 0
	}

	// invalid shard
	if uint32(shard*shardDuration) > rangeEnd {
		return 0, 0
	}

	shardStart := rangeEnd - uint32((shard+1)*shardDuration)
	shardEnd := rangeEnd - uint32(shard*shardDuration)

	// underflow protection
	if shardStart > shardEnd {
		shardStart = 0
	}

	// don't go past the start of the range
	if shardStart < rangeStart {
		shardStart = rangeStart
	}

	return shardStart, shardEnd
}
