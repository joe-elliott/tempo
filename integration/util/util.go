package util

// Collection of utilities to share between our various load tests

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/e2e"
	"github.com/jaegertracing/jaeger-idl/proto-gen/api_v2"
	thrift "github.com/jaegertracing/jaeger-idl/thrift-gen/jaeger"
	"github.com/jaegertracing/jaeger-idl/thrift-gen/zipkincore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/pdata/ptrace"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	tnoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/tempo/pkg/httpclient"
	"github.com/grafana/tempo/pkg/model/trace"
	"github.com/grafana/tempo/pkg/tempopb"
	tempoUtil "github.com/grafana/tempo/pkg/util"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
)

const (
	xScopeOrgIDHeader   = "x-scope-orgid"
	authorizationHeader = "authorization"
)

func CopyFileToSharedDir(s *e2e.Scenario, src, dst string) error {
	content, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("unable to read local file %s: %w", src, err)
	}

	_, err = writeFileToSharedDir(s, dst, content)
	return err
}

func writeFileToSharedDir(s *e2e.Scenario, dst string, content []byte) (string, error) {
	dst = filepath.Join(s.SharedDir(), dst)

	// Ensure the entire path of directories exists
	err := os.MkdirAll(filepath.Dir(dst), os.ModePerm)
	if err != nil {
		return "", err
	}

	err = os.WriteFile(dst, content, os.ModePerm)
	if err != nil {
		return "", err
	}

	return dst, nil
}

func NewOtelGRPCExporterWithAuth(endpoint, orgID, basicAuthToken string, useTLS bool) (exporter.Traces, error) {
	factory := otlpexporter.NewFactory()
	exporterCfg := factory.CreateDefaultConfig()
	otlpCfg := exporterCfg.(*otlpexporter.Config)

	// Configure headers for authentication (gRPC metadata format)
	headers := make(map[string]configopaque.String)
	if orgID != "" {
		headers[xScopeOrgIDHeader] = configopaque.String(orgID)
	}
	if basicAuthToken != "" {
		headers[authorizationHeader] = configopaque.String("Basic " + basicAuthToken)
	}

	otlpCfg.ClientConfig = configgrpc.ClientConfig{
		Endpoint: endpoint,
		TLS: configtls.ClientConfig{
			Insecure: !useTLS,
		},
		Headers: headers,
	}

	// Disable retries to get immediate error feedback
	otlpCfg.RetryConfig.Enabled = false
	// Disable queueing
	otlpCfg.QueueConfig.Enabled = false
	logger, _ := zap.NewDevelopment()
	te, err := factory.CreateTraces(
		context.Background(),
		exporter.Settings{
			ID: component.MustNewID(factory.Type().String()),
			TelemetrySettings: component.TelemetrySettings{
				Logger:         logger,
				TracerProvider: tnoop.NewTracerProvider(),
				MeterProvider:  mnoop.NewMeterProvider(),
			},
			BuildInfo: component.NewDefaultBuildInfo(),
		},
		otlpCfg,
	)
	if err != nil {
		return nil, err
	}
	err = te.Start(context.Background(), componenttest.NewNopHost())
	if err != nil {
		return nil, err
	}
	return te, nil
}

func NewOtelGRPCExporter(endpoint string) (exporter.Traces, error) {
	return NewOtelGRPCExporterWithAuth(endpoint, "", "", false)
}

func newSearchGRPCClient(ctx context.Context, endpoint string) (tempopb.StreamingQuerierClient, error) { // jpe - remove
	return NewSearchGRPCClientWithCredentials(ctx, endpoint, insecure.NewCredentials())
}

func NewSearchGRPCClientWithCredentials(ctx context.Context, endpoint string, creds credentials.TransportCredentials) (tempopb.StreamingQuerierClient, error) {
	clientConn, err := grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}

	return tempopb.NewStreamingQuerierClient(clientConn), nil
}

func SearchTraceQLAndAssertTrace(t *testing.T, client *httpclient.Client, info *tempoUtil.TraceInfo) { // jpe - so many of these stupid things. consolidate! delete!
	expected, err := info.ConstructTraceFromEpoch()
	require.NoError(t, err)

	attr := tempoUtil.RandomAttrFromTrace(expected)
	query := fmt.Sprintf(`{ .%s = "%s"}`, attr.GetKey(), attr.GetValue().GetStringValue())

	resp, err := client.SearchTraceQL(query)
	require.NoError(t, err)

	require.True(t, traceIDInResults(t, info.HexID(), resp))
}

func SearchTraceQLAndAssertTraceWithRange(t *testing.T, client *httpclient.Client, info *tempoUtil.TraceInfo, start, end int64) {
	expected, err := info.ConstructTraceFromEpoch()
	require.NoError(t, err)

	attr := tempoUtil.RandomAttrFromTrace(expected)
	query := fmt.Sprintf(`{ .%s = "%s"}`, attr.GetKey(), attr.GetValue().GetStringValue())

	resp, err := client.SearchTraceQLWithRange(query, start, end)
	require.NoError(t, err)

	require.True(t, traceIDInResults(t, info.HexID(), resp))
}

// SearchStreamAndAssertTrace will search and assert that the trace is present in the streamed results.
// nolint: revive
func SearchStreamAndAssertTrace(t *testing.T, ctx context.Context, client tempopb.StreamingQuerierClient, info *tempoUtil.TraceInfo, start, end int64) {
	expected, err := info.ConstructTraceFromEpoch()
	require.NoError(t, err)

	attr := tempoUtil.RandomAttrFromTrace(expected)
	query := fmt.Sprintf(`{ .%s = "%s"}`, attr.GetKey(), attr.GetValue().GetStringValue())

	// -- assert search
	resp, err := client.Search(ctx, &tempopb.SearchRequest{
		Query: query,
		Start: uint32(start),
		End:   uint32(end),
	})
	require.NoError(t, err)

	// drain the stream until everything is returned while watching for the trace in question
	found := false
	for {
		resp, err := resp.Recv()
		if resp != nil {
			found = traceIDInResults(t, info.HexID(), resp)
			if found {
				break
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}
	require.True(t, found)
}

func traceIDInResults(t *testing.T, hexID string, resp *tempopb.SearchResponse) bool {
	for _, s := range resp.Traces {
		equal, err := tempoUtil.EqualHexStringTraceIDs(s.TraceID, hexID)
		require.NoError(t, err)
		if equal {
			return true
		}
	}

	return false
}

func MakeThriftBatch() *thrift.Batch {
	return MakeThriftBatchWithSpanCount(1)
}

func MakeThriftBatchWithSpanCount(n int) *thrift.Batch {
	return MakeThriftBatchWithSpanCountAttributeAndName(n, "my operation", "", "y", "xx", "x")
}

func MakeThriftBatchWithSpanCountAttributeAndName(n int, name, resourceValue, spanValue, resourceTag, spanTag string) *thrift.Batch {
	var spans []*thrift.Span

	traceIDLow := rand.Int63()
	traceIDHigh := rand.Int63()
	for range n {
		spans = append(spans, &thrift.Span{
			TraceIdLow:    traceIDLow,
			TraceIdHigh:   traceIDHigh,
			SpanId:        rand.Int63(),
			ParentSpanId:  0,
			OperationName: name,
			References:    nil,
			Flags:         0,
			StartTime:     time.Now().Add(-3*time.Second).UnixNano() / 1000, // microsecconds
			Duration:      1,
			Tags: []*thrift.Tag{
				{
					Key:  spanTag,
					VStr: &spanValue,
				},
			},
			Logs: nil,
		})
	}

	return &thrift.Batch{
		Process: &thrift.Process{
			ServiceName: "my-service",
			Tags: []*thrift.Tag{
				{
					Key:   resourceTag,
					VType: thrift.TagType_STRING,
					VStr:  &resourceValue,
				},
			},
		},
		Spans: spans,
	}
}

func QueryAndAssertTrace(t *testing.T, client *httpclient.Client, info *tempoUtil.TraceInfo) {
	resp, err := client.QueryTrace(info.HexID())
	require.NoError(t, err)

	expected, err := info.ConstructTraceFromEpoch()
	require.NoError(t, err)

	AssertEqualTrace(t, resp, expected)
}

func AssertEqualTrace(t *testing.T, a, b *tempopb.Trace) {
	t.Helper()
	trace.SortTraceAndAttributes(a)
	trace.SortTraceAndAttributes(b)

	assert.Equal(t, a, b)
}

func SpanCount(a *tempopb.Trace) float64 {
	count := 0
	for _, batch := range a.ResourceSpans {
		for _, spans := range batch.ScopeSpans {
			count += len(spans.Spans)
		}
	}

	return float64(count)
}

type JaegerToOTLPExporter struct {
	exporter exporter.Traces
}

func NewJaegerToOTLPExporterWithAuth(endpoint, orgID, basicAuthToken string, useTLS bool) (*JaegerToOTLPExporter, error) {
	exp, err := NewOtelGRPCExporterWithAuth(endpoint, orgID, basicAuthToken, useTLS)
	if err != nil {
		return nil, err
	}
	return &JaegerToOTLPExporter{exporter: exp}, nil
}

func NewJaegerToOTLPExporter(endpoint string) (*JaegerToOTLPExporter, error) {
	return NewJaegerToOTLPExporterWithAuth(endpoint, "", "", false)
}

// EmitBatch converts a Jaeger Thrift batch to OpenTelemetry traces formats
// and forwards them to the configured OTLP endpoint.
func (c *JaegerToOTLPExporter) EmitBatch(ctx context.Context, b *thrift.Batch) error {
	traces, err := jaeger.ThriftToTraces(b)
	if err != nil {
		return err
	}
	return c.exporter.ConsumeTraces(ctx, traces)
}

func (c *JaegerToOTLPExporter) EmitZipkinBatch(_ context.Context, _ []*zipkincore.Span) error {
	return errors.New("EmitZipkinBatch via OTLP not implemented")
}

// JaegerGRPCExporter wraps a gRPC client that sends traces to the Jaeger gRPC receiver (port 14250)
type JaegerGRPCExporter struct {
	client    *grpc.ClientConn
	collector api_v2.CollectorServiceClient
}

// NewJaegerGRPCExporter creates a new Jaeger gRPC exporter that sends traces to the Jaeger gRPC receiver
func NewJaegerGRPCExporter(endpoint string) (*JaegerGRPCExporter, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &JaegerGRPCExporter{
		client:    conn,
		collector: api_v2.NewCollectorServiceClient(conn),
	}, nil
}

// Start implements component.Component
func (e *JaegerGRPCExporter) Start(ctx context.Context, host component.Host) error {
	return nil
}

// Shutdown implements component.Component
func (e *JaegerGRPCExporter) Shutdown(ctx context.Context) error {
	return e.client.Close()
}

// ConsumeTraces converts OTLP traces to Jaeger format and sends them via gRPC
func (e *JaegerGRPCExporter) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	batches := jaeger.ProtoFromTraces(traces)

	for _, batch := range batches {
		_, err := e.collector.PostSpans(ctx, &api_v2.PostSpansRequest{
			Batch: *batch,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// Capabilities implements consumer.Traces
func (e *JaegerGRPCExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
