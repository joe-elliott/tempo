package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/integration/util"
	tempoUtil "github.com/grafana/tempo/pkg/util"
	"github.com/grafana/tempo/tempodb/encoding"
	v2 "github.com/grafana/tempo/tempodb/encoding/v2"
)

func TestEncodings(t *testing.T) {
	const repeatedSearchCount = 10

	for _, enc := range encoding.AllEncodingsForWrites() {
		t.Run(enc.Version(), func(t *testing.T) {
			util.WithTempoHarness(t, util.TestHarnessConfig{
				ConfigOverlay: "./config-encodings.yaml",
				ConfigTemplateData: map[string]any{
					"Version": enc.Version(),
				},
				DeploymentMode: util.DeploymentModeMicroservices,
				Components:     util.ComponentsBackendQuerying | util.ComponentRecentDataQuerying,
			}, func(h *util.TempoHarness) {
				info := tempoUtil.NewTraceInfo(time.Now(), "")
				h.WriteTraceInfo(t, info)

				h.WaitTracesQueryable(t, 1)

				apiClient := h.APIClientHTTP("")
				util.QueryAndAssertTrace(t, apiClient, info)

				// v2 does not support querying and must be skipped
				if enc.Version() == v2.VersionString {
					return
				}

				// search for trace in backend multiple times with different attributes to make sure
				// we search with different scopes and with attributes from dedicated columns
				for range repeatedSearchCount {
					util.SearchTraceQLAndAssertTrace(t, apiClient, info)
				}

				h.WaitTracesWrittenToBackend(t, 1)
				h.ForceBackendQuerying(t)

				apiClient = h.APIClientHTTP("")
				grpcClient, ctx, err := h.APIClientGRPC("")
				require.NoError(t, err)

				now := time.Now()
				for range repeatedSearchCount {
					// search the backend. this works b/c we're passing a start/end AND setting query ingesters within min/max to 0
					util.SearchTraceQLAndAssertTraceWithRange(t, apiClient, info, now.Add(-time.Hour).Unix(), now.Unix())
					// find the trace with streaming. using the http server b/c that's what Grafana will do
					util.SearchStreamAndAssertTrace(t, ctx, grpcClient, info, now.Add(-time.Hour).Unix(), now.Unix())
				}
			})
		})
	}
}
