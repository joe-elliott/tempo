package frontend

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log" //nolint:all deprecated
	"github.com/grafana/dskit/user"
	"github.com/opentracing/opentracing-go"
	"github.com/segmentio/fasthash/fnv1a"

	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
)

const (
	defaultTargetBytesPerRequest = 100 * 1024 * 1024
	defaultConcurrentRequests    = 1000
)

type SearchSharderConfig struct {
	ConcurrentRequests    int           `yaml:"concurrent_jobs,omitempty"`
	TargetBytesPerRequest int           `yaml:"target_bytes_per_job,omitempty"`
	DefaultLimit          uint32        `yaml:"default_result_limit"`
	MaxLimit              uint32        `yaml:"max_result_limit"`
	MaxDuration           time.Duration `yaml:"max_duration"`
	QueryBackendAfter     time.Duration `yaml:"query_backend_after,omitempty"`
	QueryIngestersUntil   time.Duration `yaml:"query_ingesters_until,omitempty"`
	IngesterShards        int           `yaml:"ingester_shards,omitempty"`
}

type asyncSearchSharder struct {
	next pipeline.AsyncRoundTripper[combiner.PipelineResponse]

	cfg    SearchSharderConfig
	logger log.Logger
}

// newAsyncSearchSharder creates a sharding middleware for search
func newAsyncSearchSharder(cfg SearchSharderConfig, logger log.Logger) pipeline.AsyncMiddleware[combiner.PipelineResponse] {
	return pipeline.AsyncMiddlewareFunc[combiner.PipelineResponse](func(next pipeline.AsyncRoundTripper[combiner.PipelineResponse]) pipeline.AsyncRoundTripper[combiner.PipelineResponse] {
		return &asyncSearchSharder{
			next: next,

			cfg:    cfg,
			logger: logger,
		}
	})
}

// RoundTrip implements http.RoundTripper
// execute up to concurrentRequests simultaneously where each request scans ~targetMBsPerRequest
// until limit results are found
func (s *asyncSearchSharder) RoundTrip(pipelineRequest pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
	r := pipelineRequest.HTTPRequest()

	shardedSearchReq := pipelineRequest.(*shardedSearchRequest)

	// jpe all of this goes into the time range sharder?
	searchReq := shardedSearchReq.parsedRequest

	requestCtx := r.Context()
	tenantID, err := user.ExtractOrgID(requestCtx)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}
	span, ctx := opentracing.StartSpanFromContext(requestCtx, "frontend.ShardSearch") // jpe gru?
	defer span.Finish()

	// buffer allows us to insert ingestReq
	reqCh := make(chan pipeline.Request, len(shardedSearchReq.ingesterRequests))
	if len(shardedSearchReq.ingesterRequests) > 0 {
		for _, req := range shardedSearchReq.ingesterRequests {
			reqCh <- req
		}
	}

	// pass subCtx in requests so we can cancel and exit early
	go func() {
		s.buildBackendRequests(ctx, tenantID, r, searchReq, shardedSearchReq.blocks, reqCh, func(err error) {
			// todo: actually find a way to return this error to the user
			s.logger.Log("msg", "search: failed to build backend requests", "err", err)
		})
	}()

	// execute requests
	return pipeline.NewAsyncSharderChan(ctx, s.cfg.ConcurrentRequests, reqCh, nil, s.next), nil
}

// buildBackendRequests returns a slice of requests that cover all blocks in the store
// that are covered by start/end.
func (s *asyncSearchSharder) buildBackendRequests(ctx context.Context, tenantID string, parent *http.Request, searchReq *tempopb.SearchRequest, metas []*backend.BlockMeta, reqCh chan<- pipeline.Request, errFn func(error)) {
	defer close(reqCh)

	targetBytesPerRequest := s.cfg.TargetBytesPerRequest
	queryHash := hashForSearchRequest(searchReq)

	for _, m := range metas {
		pages := pagesPerRequest(m, targetBytesPerRequest)
		if pages == 0 {
			continue
		}

		blockID := m.BlockID.String()
		for startPage := 0; startPage < int(m.TotalRecords); startPage += pages {
			subR := parent.Clone(ctx)

			dc, err := m.DedicatedColumns.ToTempopb()
			if err != nil {
				errFn(fmt.Errorf("failed to convert dedicated columns. block: %s tempopb: %w", blockID, err))
				continue
			}

			// jpe - "works by accident" issue here. SearchRequest is nil on SearchBlockRequest which makes the start/end params
			//  fall through .BuildSearchBlockRequest correctly. may want to smooth this out to prevent a future bug
			subR, err = api.BuildSearchBlockRequest(subR, &tempopb.SearchBlockRequest{
				BlockID:          blockID,
				StartPage:        uint32(startPage),
				PagesToSearch:    uint32(pages),
				Encoding:         m.Encoding.String(),
				IndexPageSize:    m.IndexPageSize,
				TotalRecords:     m.TotalRecords,
				DataEncoding:     m.DataEncoding,
				Version:          m.Version,
				Size_:            m.Size,
				FooterSize:       m.FooterSize,
				DedicatedColumns: dc,
			})
			if err != nil {
				errFn(fmt.Errorf("failed to build search block request. block: %s tempopb: %w", blockID, err))
				continue
			}

			prepareRequestForQueriers(subR, tenantID)
			key := searchJobCacheKey(tenantID, queryHash, int64(searchReq.Start), int64(searchReq.End), m, startPage, pages)
			pipelineR := pipeline.NewHTTPRequest(subR)
			pipelineR.SetCacheKey(key)

			select {
			case reqCh <- pipelineR:
			case <-ctx.Done():
				// ignore the error if there is one. it will be handled elsewhere
				return
			}
		}
	}
}

// hashForSearchRequest returns a uint64 hash of the query. if the query is invalid it returns a 0 hash.
// before hashing the query is forced into a canonical form so equivalent queries will hash to the same value.
func hashForSearchRequest(searchRequest *tempopb.SearchRequest) uint64 {
	if searchRequest.Query == "" {
		return 0
	}

	ast, err := traceql.Parse(searchRequest.Query)
	if err != nil { // this should never occur. if we've made this far we've already validated the query can parse. however, for sanity, just fail to cache if we can't parse
		return 0
	}

	// forces the query into a canonical form
	query := ast.String()

	// add the query, limit and spss to the hash
	hash := fnv1a.HashString64(query)
	hash = fnv1a.AddUint64(hash, uint64(searchRequest.Limit))
	hash = fnv1a.AddUint64(hash, uint64(searchRequest.SpansPerSpanSet))

	return hash
}

// pagesPerRequest returns an integer value that indicates the number of pages
// that should be searched per query. This value is based on the target number of bytes
// 0 is returned if there is no valid answer
func pagesPerRequest(m *backend.BlockMeta, bytesPerRequest int) int {
	if m.Size == 0 || m.TotalRecords == 0 {
		return 0
	}

	bytesPerPage := m.Size / uint64(m.TotalRecords)
	if bytesPerPage == 0 {
		return 0
	}

	pagesPerQuery := bytesPerRequest / int(bytesPerPage)
	if pagesPerQuery == 0 {
		pagesPerQuery = 1 // have to have at least 1 page per query
	}

	return pagesPerQuery
}
