package util

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/grafana/dskit/backoff"
	"github.com/grafana/e2e"
)

const (
	image           = "tempo:latest"
	queryImage      = "tempo-query:latest"
	jaegerImage     = "jaegertracing/jaeger-query:1.64.0"
	prometheusImage = "prom/prometheus:latest"

	metricsTimouet = 5 * time.Second
)

func NewTempoAllInOneWithReadinessProbe(rp e2e.ReadinessProbe) *e2e.HTTPService { // jpe - get single binary working? and then add tests?
	args := []string{"-config.file=" + filepath.Join(e2e.ContainerSharedDir, "config.yaml"), "-target=all-3.0"}

	s := e2e.NewHTTPService(
		"tempo",
		image,
		e2e.NewCommandWithoutEntrypoint("/tempo", args...),
		rp,
		3200,  // http all things
		3201,  // http all things
		9095,  // grpc tempo
		14250, // jaeger grpc ingest
		9411,  // zipkin ingest (used by load)
		4317,  // otlp grpc
		4318,  // OTLP HTTP
	)

	s.SetMetricsTimeout(metricsTimouet)
	s.SetBackoff(tempoBackoff())
	return s
}

func NewTempoQuery() *e2e.HTTPService {
	args := []string{
		"-config=" + filepath.Join(e2e.ContainerSharedDir, "config-tempo-query.yaml"),
	}

	s := e2e.NewHTTPService(
		"tempo-query",
		queryImage,
		e2e.NewCommandWithoutEntrypoint("/tempo-query", args...),
		e2e.NewTCPReadinessProbe(7777),
		7777,
	)

	s.SetMetricsTimeout(metricsTimouet)
	s.SetBackoff(tempoBackoff())
	return s
}

func NewJaegerQuery() *e2e.HTTPService {
	args := []string{
		"--grpc-storage.server=tempo-query:7777",
		"--span-storage.type=grpc",
	}

	s := e2e.NewHTTPService(
		"jaeger-query",
		jaegerImage,
		e2e.NewCommandWithoutEntrypoint("/go/bin/query-linux", args...),
		e2e.NewHTTPReadinessProbe(16686, "/", 200, 299),
		16686,
	)

	s.SetMetricsTimeout(metricsTimouet)
	s.SetBackoff(tempoBackoff())
	return s
}

// newTempoService creates a Tempo service with the specified service name and target.
// serviceName is the name of the service in the e2e environment (can include replica/zone suffixes).
// target is the Tempo -target flag value (e.g., "distributor", "live-store", "querier").
// additionalPorts are optional extra ports to expose beyond the default 3200.
func newTempoService(serviceName, target string, additionalPorts ...int) *e2e.HTTPService {
	args := []string{
		"-config.file=" + filepath.Join(e2e.ContainerSharedDir, "config.yaml"),
		"-target=" + target,
	}

	s := e2e.NewHTTPService(
		serviceName,
		image,
		e2e.NewCommandWithoutEntrypoint("/tempo", args...),
		e2e.NewHTTPReadinessProbe(3200, "/ready", 200, 299),
		3200,
		additionalPorts...,
	)

	s.SetMetricsTimeout(metricsTimouet)
	s.SetBackoff(tempoBackoff())
	return s
}

func newPrometheus() *e2e.HTTPService { // jpe what can be made private?
	s := e2e.NewHTTPService(
		"prometheus",
		prometheusImage,
		e2e.NewCommandWithoutEntrypoint("/bin/prometheus", "--config.file=/etc/prometheus/prometheus.yml", "--web.enable-remote-write-receiver"),
		e2e.NewHTTPReadinessProbe(9090, "/-/ready", 200, 299),
		9090,
	)

	s.SetMetricsTimeout(metricsTimouet)
	s.SetBackoff(tempoBackoff())
	return s
}

// newAzurite creates a new Azurite service for Azure blob storage emulation
func newAzurite(port int) *e2e.HTTPService {
	s := e2e.NewHTTPService(
		"azurite",
		azuriteImage,
		e2e.NewCommandWithoutEntrypoint("sh", "-c", "azurite -l /data --blobHost 0.0.0.0"),
		e2e.NewHTTPReadinessProbe(port, "/devstoreaccount1?comp=list", 403, 403), // If we get 403 the Azurite is ready
		port, // blob storage port
	)

	s.SetBackoff(tempoBackoff())
	s.SetMetricsTimeout(metricsTimouet)
	return s
}

// newGCS creates a new fake GCS service for Google Cloud Storage emulation
func newGCS(port int) *e2e.HTTPService {
	commands := []string{
		"mkdir -p /data/tempo",
		"/bin/fake-gcs-server -data /data -public-host=tempo_e2e-gcs -port=4443",
	}
	s := e2e.NewHTTPService(
		"gcs",
		gcsImage,
		e2e.NewCommandWithoutEntrypoint("sh", "-c", strings.Join(commands, " && ")),
		e2e.NewHTTPReadinessProbe(port, "/", 400, 400), // for lack of a better way, readiness probe does not support https at the moment
		port,
	)

	s.SetMetricsTimeout(metricsTimouet)
	s.SetBackoff(tempoBackoff())
	return s
}

func tempoBackoff() backoff.Config {
	return backoff.Config{
		MinBackoff: 500 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 300, // Sometimes the CI is slow ¯\_(ツ)_/¯
	}
}
