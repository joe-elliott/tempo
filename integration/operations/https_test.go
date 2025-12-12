package deployments // jpe - rename to operations?

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/dskit/runutil"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/grafana/tempo/integration/util"
	"github.com/grafana/tempo/pkg/httpclient"
	tempoUtil "github.com/grafana/tempo/pkg/util"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
)

const (
	configHTTPS = "config-https.yaml"

	tempoPort = 3200
)

// jpe - currently just running the single binary directly. should we make a special TestHarness for this? or an option to do this?
// can we add an option to query https metrics to the e2e framework?
func TestHTTPS(t *testing.T) {
	km := setupCertificates(t)

	s, err := e2e.NewScenario("tempo_e2e_test_https")
	require.NoError(t, err)
	defer s.Close()

	kafka := e2edb.NewKafka()
	require.NoError(t, s.StartAndWaitReady(kafka), "failed to start Kafka")

	// copy in certs
	require.NoError(t, util.CopyFileToSharedDir(s, km.ServerCertFile, "tls.crt"))
	require.NoError(t, util.CopyFileToSharedDir(s, km.ServerKeyFile, "tls.key"))
	require.NoError(t, util.CopyFileToSharedDir(s, km.CaCertFile, "ca.crt"))

	require.NoError(t, util.CopyFileToSharedDir(s, configHTTPS, "config.yaml"))
	tempo := util.NewTempoAllInOneWithReadinessProbe(e2e.NewHTTPReadinessProbe(3201, "/ready", 200, 299))
	require.NoError(t, s.StartAndWaitReady(tempo))

	c, err := util.NewJaegerToOTLPExporter(tempo.Endpoint(4317))
	require.NoError(t, err)
	require.NotNil(t, c)

	// the test harness queries a metric to determine when the partition ring is
	// ready for writes, but there's no convenient way in the e2e framework to do this over
	// HTTPS, so instead we just try for a bit until a write works - jpe - add a simpler helper method in this file to do this and check for the right things
	var info *tempoUtil.TraceInfo
	require.Eventually(t, func() bool {
		info = tempoUtil.NewTraceInfo(time.Now(), "")
		err := info.EmitAllBatches(c)
		if err != nil {
			return false
		}
		return true
	}, time.Minute, 5*time.Second, "could not write trace to tempo")

	apiClient := httpclient.New("https://"+tempo.Endpoint(tempoPort), "")

	// trust bad certs
	defaultTransport := http.DefaultTransport.(*http.Transport).Clone()
	defaultTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	apiClient.WithTransport(defaultTransport)

	echoReq, err := http.NewRequest("GET", "https://"+tempo.Endpoint(tempoPort)+"/api/echo", nil)
	require.NoError(t, err)
	resp, err := apiClient.Do(echoReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// query an in-memory trace
	util.QueryAndAssertTrace(t, apiClient, info)

	// wait for the trace to be flushed to a wal block for querying. jpe - similar to above. can't use a metric :(
	time.Sleep(30 * time.Second)

	util.SearchTraceQLAndAssertTrace(t, apiClient, info)

	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
	grpcClient, err := util.NewSearchGRPCClientWithCredentials(context.Background(), tempo.Endpoint(tempoPort), creds)
	require.NoError(t, err)

	now := time.Now()
	util.SearchStreamAndAssertTrace(t, context.Background(), grpcClient, info, now.Add(-time.Hour).Unix(), now.Unix()) // jpe - add grpc client to harness
}

type keyMaterial struct {
	CaCertFile                string
	ServerCertFile            string
	ServerKeyFile             string
	ServerNoLocalhostCertFile string
	ServerNoLocalhostKeyFile  string
	ClientCA1CertFile         string
	ClientCABothCertFile      string
	Client1CertFile           string
	Client1KeyFile            string
	Client2CertFile           string
	Client2KeyFile            string
}

func setupCertificates(t *testing.T) keyMaterial {
	testCADir := t.TempDir()

	// create server side CA

	testCA := newCA("Test")
	caCertFile := filepath.Join(testCADir, "ca.crt")
	require.NoError(t, testCA.writeCACertificate(caCertFile))

	serverCertFile := filepath.Join(testCADir, "server.crt")
	serverKeyFile := filepath.Join(testCADir, "server.key")
	require.NoError(t, testCA.writeCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "server"},
			DNSNames:    []string{"localhost", "my-other-name"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		},
		serverCertFile,
		serverKeyFile,
	))

	serverNoLocalhostCertFile := filepath.Join(testCADir, "server-no-localhost.crt")
	serverNoLocalhostKeyFile := filepath.Join(testCADir, "server-no-localhost.key")
	require.NoError(t, testCA.writeCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "server-no-localhost"},
			DNSNames:    []string{"my-other-name"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		},
		serverNoLocalhostCertFile,
		serverNoLocalhostKeyFile,
	))

	// create client CAs
	testClientCA1 := newCA("Test Client CA 1")
	testClientCA2 := newCA("Test Client CA 2")

	clientCA1CertFile := filepath.Join(testCADir, "ca-client-1.crt")
	require.NoError(t, testClientCA1.writeCACertificate(clientCA1CertFile))
	clientCA2CertFile := filepath.Join(testCADir, "ca-client-2.crt")
	require.NoError(t, testClientCA2.writeCACertificate(clientCA2CertFile))

	// create a ca file with both certs
	clientCABothCertFile := filepath.Join(testCADir, "ca-client-both.crt")
	func() {
		src1, err := os.Open(clientCA1CertFile)
		require.NoError(t, err)
		defer src1.Close()
		src2, err := os.Open(clientCA2CertFile)
		require.NoError(t, err)
		defer src2.Close()

		dst, err := os.Create(clientCABothCertFile)
		require.NoError(t, err)
		defer dst.Close()

		_, err = io.Copy(dst, src1)
		require.NoError(t, err)
		_, err = io.Copy(dst, src2)
		require.NoError(t, err)
	}()

	client1CertFile := filepath.Join(testCADir, "client-1.crt")
	client1KeyFile := filepath.Join(testCADir, "client-1.key")
	require.NoError(t, testClientCA1.writeCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "client-1"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		},
		client1CertFile,
		client1KeyFile,
	))

	client2CertFile := filepath.Join(testCADir, "client-2.crt")
	client2KeyFile := filepath.Join(testCADir, "client-2.key")
	require.NoError(t, testClientCA2.writeCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "client-2"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		},
		client2CertFile,
		client2KeyFile,
	))

	return keyMaterial{
		CaCertFile:                caCertFile,
		ServerCertFile:            serverCertFile,
		ServerKeyFile:             serverKeyFile,
		ServerNoLocalhostCertFile: serverNoLocalhostCertFile,
		ServerNoLocalhostKeyFile:  serverNoLocalhostKeyFile,
		ClientCA1CertFile:         clientCA1CertFile,
		ClientCABothCertFile:      clientCABothCertFile,
		Client1CertFile:           client1CertFile,
		Client1KeyFile:            client1KeyFile,
		Client2CertFile:           client2CertFile,
		Client2KeyFile:            client2KeyFile,
	}
}

type ca struct {
	key    *ecdsa.PrivateKey
	cert   *x509.Certificate
	serial *big.Int
}

func newCA(name string) *ca {
	key, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		panic(err)
	}

	return &ca{
		key: key,
		cert: &x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject: pkix.Name{
				Organization: []string{name},
			},
			NotBefore: time.Now(),
			NotAfter:  time.Now().Add(time.Hour * 24 * 180),

			KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			BasicConstraintsValid: true,
			IsCA:                  true,
		},
		serial: big.NewInt(2),
	}
}

func writeExclusivePEMFile(path, marker string, mode os.FileMode, data []byte) (err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	defer runutil.CloseWithErrCapture(&err, f, "write pem file")

	return pem.Encode(f, &pem.Block{Type: marker, Bytes: data})
}

func (ca *ca) writeCACertificate(path string) error {
	derBytes, err := x509.CreateCertificate(rand.Reader, ca.cert, ca.cert, ca.key.Public(), ca.key)
	if err != nil {
		return err
	}

	return writeExclusivePEMFile(path, "CERTIFICATE", 0o600, derBytes)
}

func (ca *ca) writeCertificate(template *x509.Certificate, certPath string, keyPath string) error {
	key, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		return err
	}

	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}

	if err := writeExclusivePEMFile(keyPath, "PRIVATE KEY", 0o600, keyBytes); err != nil {
		return err
	}

	template.IsCA = false
	template.NotBefore = time.Now()
	if template.NotAfter.IsZero() {
		template.NotAfter = time.Now().Add(time.Hour * 24 * 180)
	}
	template.SerialNumber = ca.serial.Add(ca.serial, big.NewInt(1))

	derBytes, err := x509.CreateCertificate(rand.Reader, template, ca.cert, key.Public(), ca.key)
	if err != nil {
		return err
	}

	return writeExclusivePEMFile(certPath, "CERTIFICATE", 0o600, derBytes)
}
