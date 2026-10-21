package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/gateway"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensionsv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
)

func main() {
	if err := run(); err != nil {
		ctrl.Log.WithName("gateway").Error(err, "Could not run gateway")
		os.Exit(1)
	}
}

func run() (returnErr error) {
	var (
		address, internalAddress, subject, certFile, keyFile          string
		databaseFile, metricsAddress, metricsCertFile, metricsKeyFile string
		maxActive, maxRecords                                         int
		insecureLocal                                                 bool
	)
	flag.StringVar(&address, "gateway-address", os.Getenv("GATEWAY_ADDRESS"),
		"Public HTTP listener; optional TLS with gateway-cert and gateway-key")
	flag.StringVar(&internalAddress, "gateway-internal-address", os.Getenv("GATEWAY_INTERNAL_ADDRESS"),
		"Controller RPC HTTP listener; optional TLS with gateway-cert and gateway-key")
	defaultSubject := os.Getenv("GATEWAY_SUBJECT")
	if defaultSubject == "" {
		defaultSubject = "gateway"
	}
	flag.StringVar(&subject, "gateway-subject", defaultSubject,
		"Stable service identity for durable replay; not authentication")
	flag.StringVar(&certFile, "gateway-cert", os.Getenv("GATEWAY_CERT"), "TLS certificate")
	flag.StringVar(&keyFile, "gateway-key", os.Getenv("GATEWAY_KEY"), "TLS private key")
	flag.StringVar(&metricsCertFile, "gateway-metrics-cert", os.Getenv("GATEWAY_METRICS_CERT"), "Metrics TLS certificate")
	flag.StringVar(&metricsKeyFile, "gateway-metrics-key", os.Getenv("GATEWAY_METRICS_KEY"), "Metrics TLS private key")
	flag.StringVar(&databaseFile, "gateway-database-file", os.Getenv("GATEWAY_DATABASE_FILE"),
		"Required Secret-mounted PostgreSQL DSN")
	flag.IntVar(&maxActive, "gateway-max-active", 32, "Database active execution quota")
	flag.IntVar(&maxRecords, "gateway-max-records", 100000, "Database retained identity quota")
	flag.BoolVar(&insecureLocal, "gateway-database-insecure-local", false, "Allow plaintext disposable local database")
	defaultMetrics := os.Getenv("GATEWAY_METRICS_ADDRESS")
	if defaultMetrics == "" {
		defaultMetrics = "0"
	}
	flag.StringVar(&metricsAddress, "metrics-bind-address", defaultMetrics,
		"Kubernetes-authorized HTTPS metrics listener; 0 disables it")
	logOptions := zap.Options{Development: true}
	logOptions.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&logOptions)))
	if address == "" || internalAddress == "" {
		return errors.New("gateway listener configuration required")
	}
	if (certFile == "") != (keyFile == "") {
		return errors.New("gateway TLS requires both certificate and key")
	}
	if databaseFile == "" {
		return errors.New("gateway database configuration required")
	}
	raw, readErr := os.ReadFile(databaseFile)
	if readErr != nil {
		return errors.New("read gateway database Secret")
	}
	store, err := gateway.OpenPostgreSQL(context.Background(), gateway.PostgreSQLConfig{
		DSN: strings.TrimSpace(string(raw)), MaxActive: maxActive,
		MaxRecords: maxRecords, AllowInsecureLocal: insecureLocal})
	if err != nil {
		return fmt.Errorf("open gateway store: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, store.Close()) }()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(agentsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(sandboxv1beta1.AddToScheme(scheme))
	utilruntime.Must(extensionsv1beta1.AddToScheme(scheme))
	config, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes configuration: %w", err)
	}
	k8s, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("configure Kubernetes client: %w", err)
	}
	provider, err := openaiapi.NewClientWithMetrics(ctrlmetrics.Registry)
	if err != nil {
		return fmt.Errorf("configure OpenAI client: %w", err)
	}
	g, err := gateway.New(store, k8s, provider, subject, nil)
	if err != nil {
		return fmt.Errorf("configure gateway: %w", err)
	}
	if err := ctrlmetrics.Registry.Register(g.Metrics()); err != nil {
		return fmt.Errorf("register gateway metrics: %w", err)
	}
	if err := g.EnableSessionManagement(k8s); err != nil {
		return fmt.Errorf("configure gateway Session management: %w", err)
	}
	if err := g.EnableProfiles(k8s); err != nil {
		return fmt.Errorf("configure Kubernetes Profiles: %w", err)
	}
	internalHandler := g.InternalHandler()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if metricsCertFile == "" && metricsKeyFile == "" {
		metricsCertFile, metricsKeyFile = certFile, keyFile
	}
	metricServer, err := secureMetricsServer(metricsAddress, metricsCertFile, metricsKeyFile, config)
	if err != nil {
		return fmt.Errorf("configure secure gateway metrics: %w", err)
	}
	internal := &gateway.InternalServer{Handler: internalHandler, Address: internalAddress,
		CertFile: certFile, KeyFile: keyFile}
	public := &gateway.Server{Gateway: g, Address: address, CertFile: certFile, KeyFile: keyFile}
	listeners := []listener{internal, public}
	if metricServer != nil {
		listeners = append(listeners, metricServer)
	}
	return serveListeners(ctx, stop, listeners)
}

type listener interface{ Start(context.Context) error }

// An exit from any listener stops and joins every other listener before the
// gateway's deferred database cleanup can run.
func serveListeners(ctx context.Context, stop context.CancelFunc, listeners []listener) error {
	if len(listeners) == 0 {
		return errors.New("gateway listeners are required")
	}
	results := make(chan error, len(listeners)) // Each owned listener reports once.
	for _, server := range listeners {
		go func() { results <- server.Start(ctx) }()
	}
	serveErr := <-results
	stop()
	for i := 1; i < len(listeners); i++ {
		serveErr = errors.Join(serveErr, <-results)
	}
	if serveErr != nil {
		return fmt.Errorf("serve gateway: %w", serveErr)
	}
	return nil
}

func secureMetricsServer(address, certFile, keyFile string, config *rest.Config) (metricsserver.Server, error) {
	if address == "0" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, errors.New("metrics TLS certificate and key required")
	}
	if filepath.Dir(certFile) != filepath.Dir(keyFile) {
		return nil, errors.New("metrics certificate and key must share a mounted directory")
	}
	httpClient, err := rest.HTTPClientFor(config)
	if err != nil {
		return nil, fmt.Errorf("configure metrics Kubernetes client: %w", err)
	}
	return metricsserver.NewServer(metricsserver.Options{
		BindAddress: address, SecureServing: true,
		CertDir: filepath.Dir(certFile), CertName: filepath.Base(certFile), KeyName: filepath.Base(keyFile),
		FilterProvider: filters.WithAuthenticationAndAuthorization,
		TLSOpts:        []func(*tls.Config){func(config *tls.Config) { config.NextProtos = []string{"http/1.1"} }},
	}, config, httpClient)
}
