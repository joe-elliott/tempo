package util

import (
	"context"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/grafana/tempo/pkg/httpclient"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/util"
	"github.com/jaegertracing/jaeger-idl/thrift-gen/jaeger"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func (h *TempoHarness) APIClientHTTP(tenant string) *httpclient.Client {
	endpoint := h.Services[ServiceQueryFrontend].Endpoint(3200)

	return httpclient.New("http://"+endpoint, tenant)
}

func (h *TempoHarness) APIClientGRPC(tenant string) (tempopb.StreamingQuerierClient, context.Context, error) {
	endpoint := h.Services[ServiceQueryFrontend].Endpoint(3200)

	ctx := context.Background()

	if tenant != "" {
		ctx = user.InjectOrgID(ctx, tenant)
		var err error
		ctx, err = user.InjectIntoGRPCRequest(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	client, err := newSearchGRPCClient(ctx, endpoint)
	if err != nil {
		return nil, nil, err
	}
	return client, ctx, nil
}

func (h *TempoHarness) WriteTraceInfo(t *testing.T, traceInfo *util.TraceInfo) {
	exporter, err := h.exporterJaeger()
	require.NoError(t, err)
	require.NoError(t, traceInfo.EmitAllBatches(exporter))
}

func (h *TempoHarness) WriteJaegerBatch(t *testing.T, batch *jaeger.Batch) {
	exporter, err := h.exporterJaeger()
	require.NoError(t, err)
	require.NoError(t, exporter.EmitBatch(context.Background(), batch))
}

func (h *TempoHarness) WriteOTLPTraces(t *testing.T, traces ptrace.Traces) {
	exporter, err := h.exporterOTLP()
	require.NoError(t, err)
	require.NoError(t, exporter.ConsumeTraces(context.Background(), traces))
	require.NoError(t, exporter.Shutdown(context.Background()))
}

func (h *TempoHarness) exporterJaeger() (*JaegerToOTLPExporter, error) {
	exporter, err := NewJaegerToOTLPExporter(h.DistributorOTLPEndpoint)
	if err != nil {
		return nil, err
	}
	return exporter, nil
}

func (h *TempoHarness) exporterOTLP() (exporter.Traces, error) {
	exporter, err := NewOtelGRPCExporter(h.DistributorOTLPEndpoint)
	if err != nil {
		return nil, err
	}
	return exporter, nil
}
