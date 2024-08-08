package frontend

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gogo/protobuf/jsonpb"
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
	ingesterRequests []pipeline.Request
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
		return &asyncTimeRangeSearchSharder{
			next:      next,
			reader:    reader,
			cfg:       cfg,
			overrides: o,
		}
	})
}

func (s *asyncTimeRangeSearchSharder) RoundTrip(pipelineRequest pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
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

	// metas are returned in reverse order by end time. metas have to be pulled all at once and evaluated to be in or outside the range
	// if we evaluate multiple times in a lower roundtripper we could get different views of the backend
	metas := s.reader.BlockMetas(tenantID)
	metas = s.blockMetas(metas, searchReq.Start, searchReq.End)

	ingesterRequests, err := s.ingesterRequests(ctx, tenantID, r, searchReq)
	if err != nil {
		return nil, err
	}

	// jpe calc and send total jobs, etc off of the block metas

	// if the start and end are 0 then we are searching ingesters only and the time sharding doesn't matter,
	// just pass to the next middleware
	if len(metas) == 0 && len(ingesterRequests) > 0 {
		return s.next.RoundTrip(&shardedSearchRequest{ // jpe to pointer or not to pointer
			Request:          pipelineRequest,
			parsedRequest:    searchReq,
			blocks:           nil,
			ingesterRequests: ingesterRequests,
		})
	}

	// get backned stats
	totalJobs, totalBlocks, totalBlockBytes := s.backendStats(metas)
	totalJobs += len(ingesterRequests)

	asyncResponseSender := pipeline.NewAsyncResponseSender()

	go func() {
		defer asyncResponseSender.SendComplete()

		// send a job to communicate the search metrics. this is consumed by the combiner to calculate totalblocks/bytes/jobs
		var jobMetricsResponse pipeline.Responses[combiner.PipelineResponse] // jpe turn this into a meta pipeline item
		if totalJobs > 0 {
			resp := &tempopb.SearchResponse{ // jpe - turn into metadata?
				Metrics: &tempopb.SearchMetrics{
					TotalBlocks:     uint32(totalBlocks),
					TotalBlockBytes: totalBlockBytes,
					TotalJobs:       uint32(totalJobs),
				},
			}

			m := jsonpb.Marshaler{}
			body, err := m.MarshalToString(resp)
			if err != nil {
				panic(err) // jpe! this will disappear when its a meta response
			}

			jobMetricsResponse = pipeline.NewSuccessfulResponse(body)
			asyncResponseSender.Send(ctx, jobMetricsResponse)
		}

		for len(metas) > 0 {
			blocksPerShard := 10 // jpe - const
			if len(metas) < blocksPerShard {
				blocksPerShard = len(metas)
			}
			shardMetas := metas[:blocksPerShard]
			metas = metas[blocksPerShard:]

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

			ingesterRequests = nil

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
	// calculate duration (start and end) to search the backend blocks
	start, end = backendRange(start, end, s.cfg.QueryBackendAfter)

	// no need to search backend
	if start == end {
		return nil
	}

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

// ingesterRequest returns a new start and end time range for the backend as well as an http request
// that covers the ingesters. If nil is returned for the http.Request then there is no ingesters query.
// since this function modifies searchReq.Start and End we are taking a value instead of a pointer to prevent it from
// unexpectedly changing the passed searchReq.
func (s *asyncTimeRangeSearchSharder) ingesterRequests(ctx context.Context, tenantID string, parent *http.Request, searchReq *tempopb.SearchRequest) ([]pipeline.Request, error) {
	// request without start or end, search only in ingester
	if searchReq.Start == 0 || searchReq.End == 0 {
		req, err := buildIngesterRequest(ctx, tenantID, parent, searchReq)
		if err != nil {
			return nil, err
		}
		return []pipeline.Request{req}, nil
	}

	ingesterUntil := uint32(time.Now().Add(-s.cfg.QueryIngestersUntil).Unix())

	// if there's no overlap between the query and ingester range just return nil
	if searchReq.End < ingesterUntil {
		return nil, nil
	}

	ingesterStart := searchReq.Start
	ingesterEnd := searchReq.End

	// adjust ingesterStart if necessary
	if ingesterStart < ingesterUntil {
		ingesterStart = ingesterUntil
	}

	// if ingester start == ingester end then we don't need to query it
	if ingesterStart == ingesterEnd {
		return nil, nil
	}

	searchReq.Start = ingesterStart
	searchReq.End = ingesterEnd

	// Split the start and end range into sub requests for each range.
	duration := searchReq.End - searchReq.Start
	interval := duration / uint32(s.cfg.IngesterShards)
	intervalMinimum := uint32(60)

	if interval < intervalMinimum {
		interval = intervalMinimum
	}

	requests := make([]pipeline.Request, 0, 1)
	for i := 0; i < s.cfg.IngesterShards; i++ {
		var (
			subReq     = searchReq
			shardStart = ingesterStart + uint32(i)*interval
			shardEnd   = shardStart + interval
		)

		// stop if we've gone past the end of the range
		if shardStart >= ingesterEnd {
			break
		}

		// snap shardEnd to the end of the query range
		if shardEnd >= ingesterEnd || i == s.cfg.IngesterShards-1 {
			shardEnd = ingesterEnd
		}

		subReq.Start = shardStart
		subReq.End = shardEnd

		req, err := buildIngesterRequest(ctx, tenantID, parent, subReq)
		if err != nil {
			return nil, err
		}

		requests = append(requests, req)
	}

	return requests, nil
}

// backendRequest builds backend requests to search backend blocks. backendRequest takes ownership of reqCh and closes it.
// it returns 3 int values: totalBlocks, totalBlockBytes, and estimated jobs
func (s *asyncTimeRangeSearchSharder) backendStats(metas []*backend.BlockMeta) (totalJobs, totalBlocks int, totalBlockBytes uint64) {
	targetBytesPerRequest := s.cfg.TargetBytesPerRequest

	// calculate metrics to return to the caller
	totalBlocks = len(metas)
	for _, b := range metas {
		p := pagesPerRequest(b, targetBytesPerRequest)

		totalJobs += int(b.TotalRecords) / p
		if int(b.TotalRecords)%p != 0 {
			totalJobs++
		}
		totalBlockBytes += b.Size
	}

	return
}

func buildIngesterRequest(ctx context.Context, tenantID string, parent *http.Request, searchReq *tempopb.SearchRequest) (pipeline.Request, error) {
	subR := parent.Clone(ctx)
	subR, err := api.BuildSearchRequest(subR, searchReq)
	if err != nil {
		return nil, err
	}

	prepareRequestForQueriers(subR, tenantID)
	return pipeline.NewHTTPRequest(subR), nil
}

// backendRange returns a new start/end range for the backend based on the config parameter
// query_backend_after. If the returned start == the returned end then backend querying is not necessary.
func backendRange(start, end uint32, queryBackendAfter time.Duration) (uint32, uint32) {
	now := time.Now()
	backendAfter := uint32(now.Add(-queryBackendAfter).Unix())

	// adjust start/end if necessary. if the entire query range was inside backendAfter then
	// start will == end. This signals we don't need to query the backend.
	if end > backendAfter {
		end = backendAfter
	}
	if start > backendAfter {
		start = backendAfter
	}

	return start, end
}
