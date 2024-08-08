package frontend

// jpe - new tests?
// func TestAsyncTimeRangeSearchSharder(t *testing.T) {
// 	tests := []struct {
// 		name           string
// 		requestUrl     string
// 		expectedShards [][]uint32
// 	}{
// 		{
// 			name:           "ingester only search",
// 			requestUrl:     "/api/search?start=0&end=0",
// 			expectedShards: [][]uint32{{0, 0}},
// 		},
// 		{
// 			name:           "1 shard",
// 			requestUrl:     "/api/search?start=100&end=3700",
// 			expectedShards: [][]uint32{{100, 3700}},
// 		},
// 		{
// 			name:           "lots of shards!",
// 			requestUrl:     "/api/search?start=1000&end=10000",
// 			expectedShards: [][]uint32{{6400, 10000}, {2800, 6400}, {1000, 2800}},
// 		},
// 	}

// 	for _, tc := range tests {
// 		t.Run(tc.name, func(t *testing.T) {
// 			req := httptest.NewRequest("GET", tc.requestUrl, nil)

// 			actualShards := [][]uint32{}

// 			mtx := sync.Mutex{}
// 			test := newAsyncTimeRangeSearchSharder()
// 			next := pipeline.AsyncRoundTripperFunc[combiner.PipelineResponse](func(req pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
// 				shardedRequest := req.(*shardedSearchRequest)

// 				mtx.Lock()
// 				defer mtx.Unlock()

// 				actualShards = append(actualShards, []uint32{shardedRequest.shardStart, shardedRequest.shardEnd})

// 				// return a dummy response that parses
// 				resString, err := (&jsonpb.Marshaler{}).MarshalToString(&tempopb.SearchResponse{
// 					Metrics: &tempopb.SearchMetrics{},
// 				})
// 				require.NoError(t, err)

// 				return pipeline.NewHTTPToAsyncResponse(&http.Response{
// 					Body:       io.NopCloser(strings.NewReader(resString)),
// 					StatusCode: 200,
// 				}), nil
// 			})

// 			rt := test.Wrap(next)
// 			resps, err := rt.RoundTrip(pipeline.NewHTTPRequest(req))
// 			require.NoError(t, err)

// 			// drain all responses
// 			for {
// 				_, done, err := resps.Next(context.Background())
// 				require.NoError(t, err)
// 				if done {
// 					break
// 				}
// 			}

// 			// sort and compare
// 			slices.SortFunc(actualShards, func(i, j []uint32) int {
// 				return int(j[0] - i[0])
// 			})
// 			require.Equal(t, tc.expectedShards, actualShards)
// 		})
// 	}
// }

// func TestShardTotal(t *testing.T) {
// 	tcs := []struct {
// 		start    uint32
// 		end      uint32
// 		expected int
// 	}{
// 		{start: 0, end: 0, expected: 0},
// 		{start: 200, end: 100, expected: 0}, // start >= end (invalid)
// 		{start: 200, end: 200, expected: 0}, // jpe is this invalid?
// 		{start: 0, end: 200, expected: 1},
// 		{start: 0, end: 3600, expected: 1},
// 		{start: 0, end: 3601, expected: 2},
// 		{start: 0, end: 3600*10 + 3, expected: 11},
// 		{start: 0, end: 3600 * 15, expected: 15},
// 		{start: 2342234, end: 2342234 + (3600 * 12) + 3, expected: 13},
// 	}

// 	for _, tc := range tcs {
// 		t.Run(fmt.Sprintf("%d-%d", tc.start, tc.end), func(t *testing.T) {
// 			actual := shardTotal(tc.start, tc.end)
// 			require.Equal(t, tc.expected, actual)
// 		})
// 	}
// }

// func TestShardStartEnd(t *testing.T) {
// 	tcs := []struct {
// 		start         uint32
// 		end           uint32
// 		shard         int
// 		expectedStart uint32
// 		expectedEnd   uint32
// 	}{
// 		{start: 0, end: 0, shard: 0, expectedStart: 0, expectedEnd: 0}, // invalid range
// 		{start: 0, end: 10, shard: 0, expectedStart: 0, expectedEnd: 10},
// 		{start: 0, end: 10, shard: 1, expectedStart: 0, expectedEnd: 0},  // invalid shard
// 		{start: 0, end: 10, shard: -1, expectedStart: 0, expectedEnd: 0}, // invalid shard
// 		// 0 based - evenly divisible range
// 		{start: 0, end: 7200, shard: 0, expectedStart: 3600, expectedEnd: 7200},
// 		{start: 0, end: 7200, shard: 1, expectedStart: 0, expectedEnd: 3600},
// 		// 100 based - evenly divisible range
// 		{start: 100, end: 7300, shard: 0, expectedStart: 3700, expectedEnd: 7300},
// 		{start: 100, end: 7300, shard: 1, expectedStart: 100, expectedEnd: 3700},
// 		// 100 based
// 		{start: 100, end: 7700, shard: 0, expectedStart: 4100, expectedEnd: 7700},
// 		{start: 100, end: 7700, shard: 1, expectedStart: 500, expectedEnd: 4100},
// 		{start: 100, end: 7700, shard: 2, expectedStart: 100, expectedEnd: 500},
// 		{start: 100, end: 7700, shard: 3, expectedStart: 0, expectedEnd: 0}, // invalid shard
// 	}

// 	for _, tc := range tcs {
// 		t.Run(fmt.Sprintf("%d-%d-%d", tc.start, tc.end, tc.shard), func(t *testing.T) {
// 			actualStart, actualEnd := shardStartEnd(tc.shard, tc.start, tc.end)
// 			require.Equal(t, int(tc.expectedStart), int(actualStart))
// 			require.Equal(t, int(tc.expectedEnd), int(actualEnd))
// 		})
// 	}
// }
