package traceql

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	v1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/grafana/tempo/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestCombineResults(t *testing.T) {
	tcs := []struct {
		name     string
		existing *tempopb.TraceSearchMetadata
		new      *tempopb.TraceSearchMetadata
		expected *tempopb.TraceSearchMetadata
	}{
		{
			name: "overwrite nothing",
			existing: &tempopb.TraceSearchMetadata{
				SpanSet:  &tempopb.SpanSet{},
				SpanSets: []*tempopb.SpanSet{},
			},
			new: &tempopb.TraceSearchMetadata{
				TraceID:           "trace-1",
				RootServiceName:   "service-1",
				RootTraceName:     "root-trace-1",
				StartTimeUnixNano: 123,
				DurationMs:        100,
				SpanSets:          []*tempopb.SpanSet{},
			},
			expected: &tempopb.TraceSearchMetadata{
				TraceID:           "trace-1",
				RootServiceName:   "service-1",
				RootTraceName:     "root-trace-1",
				StartTimeUnixNano: 123,
				DurationMs:        100,
				SpanSets:          []*tempopb.SpanSet{},
			},
		},
		{
			name: "mixed copying in fields",
			existing: &tempopb.TraceSearchMetadata{
				TraceID:           "existing-trace",
				RootServiceName:   "existing-service",
				RootTraceName:     "existing-root-trace",
				StartTimeUnixNano: 100,
				DurationMs:        200,
				SpanSets:          []*tempopb.SpanSet{},
			},
			new: &tempopb.TraceSearchMetadata{
				TraceID:           "new-trace",
				RootServiceName:   "new-service",
				RootTraceName:     "new-root-trace",
				StartTimeUnixNano: 150,
				DurationMs:        300,
				SpanSets:          []*tempopb.SpanSet{},
			},
			expected: &tempopb.TraceSearchMetadata{
				TraceID:           "existing-trace",
				RootServiceName:   "existing-service",
				RootTraceName:     "existing-root-trace",
				StartTimeUnixNano: 100,
				DurationMs:        300,
				SpanSets:          []*tempopb.SpanSet{},
			},
		},
		{
			name: "copy in spansets",
			existing: &tempopb.TraceSearchMetadata{
				SpanSet:  &tempopb.SpanSet{},
				SpanSets: []*tempopb.SpanSet{},
			},
			new: &tempopb.TraceSearchMetadata{
				SpanSets: []*tempopb.SpanSet{
					{
						Matched:    3,
						Spans:      []*tempopb.Span{{SpanID: "span-1"}},
						Attributes: []*v1.KeyValue{{Key: "avg(test)", Value: &v1.AnyValue{Value: &v1.AnyValue_DoubleValue{DoubleValue: 1}}}},
					},
				},
			},
			expected: &tempopb.TraceSearchMetadata{
				SpanSets: []*tempopb.SpanSet{
					{
						Matched:    3,
						Spans:      []*tempopb.Span{{SpanID: "span-1"}},
						Attributes: []*v1.KeyValue{{Key: "avg(test)", Value: &v1.AnyValue{Value: &v1.AnyValue_DoubleValue{DoubleValue: 1}}}},
					},
				},
			},
		},
		{
			name: "take higher matches",
			existing: &tempopb.TraceSearchMetadata{
				SpanSet: &tempopb.SpanSet{},
				SpanSets: []*tempopb.SpanSet{
					{
						Matched:    3,
						Spans:      []*tempopb.Span{{SpanID: "span-1"}},
						Attributes: []*v1.KeyValue{{Key: "avg(test)", Value: &v1.AnyValue{Value: &v1.AnyValue_DoubleValue{DoubleValue: 1}}}},
					},
				},
			},
			new: &tempopb.TraceSearchMetadata{
				SpanSets: []*tempopb.SpanSet{
					{
						Matched:    5,
						Spans:      []*tempopb.Span{{SpanID: "span-2"}},
						Attributes: []*v1.KeyValue{{Key: "avg(test)", Value: &v1.AnyValue{Value: &v1.AnyValue_DoubleValue{DoubleValue: 3}}}},
					},
				},
			},
			expected: &tempopb.TraceSearchMetadata{
				SpanSets: []*tempopb.SpanSet{
					{
						Matched:    5,
						Spans:      []*tempopb.Span{{SpanID: "span-2"}},
						Attributes: []*v1.KeyValue{{Key: "avg(test)", Value: &v1.AnyValue{Value: &v1.AnyValue_DoubleValue{DoubleValue: 3}}}},
					},
				},
			},
		},
		{
			name: "keep higher matches",
			existing: &tempopb.TraceSearchMetadata{
				SpanSet: &tempopb.SpanSet{},
				SpanSets: []*tempopb.SpanSet{
					{
						Matched:    7,
						Spans:      []*tempopb.Span{{SpanID: "span-1"}},
						Attributes: []*v1.KeyValue{{Key: "avg(test)", Value: &v1.AnyValue{Value: &v1.AnyValue_DoubleValue{DoubleValue: 1}}}},
					},
				},
			},
			new: &tempopb.TraceSearchMetadata{
				SpanSets: []*tempopb.SpanSet{
					{
						Matched:    5,
						Spans:      []*tempopb.Span{{SpanID: "span-2"}},
						Attributes: []*v1.KeyValue{{Key: "avg(test)", Value: &v1.AnyValue{Value: &v1.AnyValue_DoubleValue{DoubleValue: 3}}}},
					},
				},
			},
			expected: &tempopb.TraceSearchMetadata{
				SpanSets: []*tempopb.SpanSet{
					{
						Matched:    7,
						Spans:      []*tempopb.Span{{SpanID: "span-1"}},
						Attributes: []*v1.KeyValue{{Key: "avg(test)", Value: &v1.AnyValue{Value: &v1.AnyValue_DoubleValue{DoubleValue: 1}}}},
					},
				},
			},
		},
		{
			name: "respect by()",
			existing: &tempopb.TraceSearchMetadata{
				SpanSet: &tempopb.SpanSet{},
				SpanSets: []*tempopb.SpanSet{
					{
						Matched:    7,
						Spans:      []*tempopb.Span{{SpanID: "span-1"}},
						Attributes: []*v1.KeyValue{{Key: "by(name)", Value: &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: "a"}}}},
					},
					{
						Matched:    3,
						Spans:      []*tempopb.Span{{SpanID: "span-1"}},
						Attributes: []*v1.KeyValue{{Key: "by(duration)", Value: &v1.AnyValue{Value: &v1.AnyValue_DoubleValue{DoubleValue: 1.1}}}},
					},
				},
			},
			new: &tempopb.TraceSearchMetadata{
				SpanSets: []*tempopb.SpanSet{
					{
						Matched:    5,
						Spans:      []*tempopb.Span{{SpanID: "span-2"}},
						Attributes: []*v1.KeyValue{{Key: "by(name)", Value: &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: "a"}}}},
					},
				},
			},
			expected: &tempopb.TraceSearchMetadata{
				SpanSets: []*tempopb.SpanSet{
					{
						Matched:    7,
						Spans:      []*tempopb.Span{{SpanID: "span-1"}},
						Attributes: []*v1.KeyValue{{Key: "by(name)", Value: &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: "a"}}}},
					},
					{
						Matched:    3,
						Spans:      []*tempopb.Span{{SpanID: "span-1"}},
						Attributes: []*v1.KeyValue{{Key: "by(duration)", Value: &v1.AnyValue{Value: &v1.AnyValue_DoubleValue{DoubleValue: 1.1}}}},
					},
				},
			},
		},
		{
			name: "merge ServiceStats",
			existing: &tempopb.TraceSearchMetadata{
				ServiceStats: map[string]*tempopb.ServiceStats{
					"service1": {
						SpanCount:  5,
						ErrorCount: 1,
					},
				},
			},
			new: &tempopb.TraceSearchMetadata{
				ServiceStats: map[string]*tempopb.ServiceStats{
					"service1": {
						SpanCount:  3,
						ErrorCount: 2,
					},
				},
			},
			expected: &tempopb.TraceSearchMetadata{
				ServiceStats: map[string]*tempopb.ServiceStats{
					"service1": {
						SpanCount:  5,
						ErrorCount: 2,
					},
				},
			},
		},
		{
			name:     "existing ServiceStats is nil doesn't panic",
			existing: &tempopb.TraceSearchMetadata{},
			new: &tempopb.TraceSearchMetadata{
				ServiceStats: map[string]*tempopb.ServiceStats{
					"service1": {
						SpanCount:  3,
						ErrorCount: 2,
					},
				},
			},
			expected: &tempopb.TraceSearchMetadata{
				ServiceStats: map[string]*tempopb.ServiceStats{
					"service1": {
						SpanCount:  3,
						ErrorCount: 2,
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			combineSearchResults(tc.existing, tc.new)

			// confirm that the SpanSet on tc.existing is contained in the slice of SpanSets
			// then nil out. the actual spanset chosen is based on map iteration order
			found := len(tc.existing.SpanSets) == 0
			for _, ss := range tc.existing.SpanSets {
				if ss == tc.existing.SpanSet {
					found = true
					break
				}
			}
			require.True(t, found)
			tc.expected.SpanSet = nil
			tc.existing.SpanSet = nil

			require.Equal(t, tc.expected, tc.existing)
		})
	}
}

func TestCombinerKeepsMostRecent(t *testing.T) {
	totalTraces := 10
	keepMostRecent := 5
	combiner := NewMetadataCombiner(keepMostRecent, true).(*mostRecentCombiner)

	// make traces
	traces := make([]*Spanset, totalTraces)
	for i := 0; i < totalTraces; i++ {
		traceID, err := util.HexStringToTraceID(fmt.Sprintf("%d", i))
		require.NoError(t, err)

		traces[i] = &Spanset{
			TraceID:            traceID,
			StartTimeUnixNanos: uint64(i) * uint64(time.Second),
		}
	}

	// save off the most recent and reverse b/c the combiner returns most recent first
	expected := make([]*tempopb.TraceSearchMetadata, 0, keepMostRecent)
	for i := totalTraces - keepMostRecent; i < totalTraces; i++ {
		expected = append(expected, asTraceSearchMetadata(traces[i]))
	}
	slices.Reverse(expected)

	rand.Shuffle(totalTraces, func(i, j int) {
		traces[i], traces[j] = traces[j], traces[i]
	})

	// add to combiner
	for i := 0; i < totalTraces; i++ {
		combiner.addSpanset(traces[i])
	}

	// test that the most recent are kept
	actual := combiner.Metadata()
	require.Equal(t, expected, actual)
	require.Equal(t, keepMostRecent, combiner.Count())
	require.Equal(t, expected[len(expected)-1].StartTimeUnixNano, combiner.OldestTimestampNanos())
	for _, tr := range expected {
		require.True(t, combiner.Exists(tr.TraceID))
	}

	// test MetadataAfter. 10 traces are added with start times 0-9. We want to get all traces that started after 7
	afterSeconds := uint32(7)
	expectedTracesCount := totalTraces - int(afterSeconds+1)
	actualTraces := combiner.MetadataAfter(afterSeconds)
	require.Equal(t, expectedTracesCount, len(actualTraces))
}
