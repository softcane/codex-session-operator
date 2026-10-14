package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
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
)

func main() {
	if err := run(); err != nil {
		ctrl.Log.WithName("gateway").Error(err, "Could not run gateway")
		os.Exit(1)
	}
}

func run() (returnErr error) {
	var (
		address, internalAddress, authFile, internalTokenFile, certFile, keyFile string
		issueRoutesFile, workRoutesFile, databaseFile, metricsAddress            string
		maxActive, maxRecords                                                    int
		insecureLocal                                                            bool
	)
	flag.StringVar(&address, "gateway-address", os.Getenv("GATEWAY_ADDRESS"), "Public HTTPS listener")
	flag.StringVar(&internalAddress, "gateway-internal-address", os.Getenv("GATEWAY_INTERNAL_ADDRESS"),
		"Controller RPC HTTPS listener")
	flag.StringVar(&authFile, "gateway-auth-file", os.Getenv("GATEWAY_AUTH_FILE"), "Secret-mounted caller principals")
	flag.StringVar(&internalTokenFile, "gateway-internal-token-file", os.Getenv("GATEWAY_INTERNAL_TOKEN_FILE"),
		"Secret-mounted controller token")
	flag.StringVar(&certFile, "gateway-cert", os.Getenv("GATEWAY_CERT"), "TLS certificate")
	flag.StringVar(&keyFile, "gateway-key", os.Getenv("GATEWAY_KEY"), "TLS private key")
	flag.StringVar(&issueRoutesFile, "issue-routes-file", os.Getenv("ISSUE_ROUTES_FILE"), "Optional GitHub issue routes")
	flag.StringVar(&workRoutesFile, "work-routes-file", os.Getenv("WORK_ROUTES_FILE"),
		"Optional trusted work event routes")
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
	if address == "" || internalAddress == "" || certFile == "" || keyFile == "" {
		return errors.New("gateway listener and TLS configuration required")
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
	principalJSON, principalErr := os.ReadFile(authFile)
	internalToken, tokenErr := os.ReadFile(internalTokenFile)
	if principalErr != nil || tokenErr != nil {
		return fmt.Errorf("read gateway authentication configuration: %w", errors.Join(principalErr, tokenErr))
	}
	var principals []gateway.Principal
	if err := json.Unmarshal(principalJSON, &principals); err != nil {
		return fmt.Errorf("parse gateway authentication configuration: %w", err)
	}
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(agentsv1alpha1.AddToScheme(scheme))
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
	g, err := gateway.New(store, k8s, provider, principals, nil)
	if err != nil {
		return fmt.Errorf("configure gateway: %w", err)
	}
	if err := ctrlmetrics.Registry.Register(g.Metrics()); err != nil {
		return fmt.Errorf("register gateway metrics: %w", err)
	}
	if err := g.EnableSessionManagement(k8s); err != nil {
		return fmt.Errorf("configure gateway Session management: %w", err)
	}
	if workRoutesFile != "" {
		raw, readErr := os.ReadFile(workRoutesFile)
		if readErr != nil {
			return fmt.Errorf("read work routing configuration: %w", readErr)
		}
		routes, parseErr := gateway.ParseWorkRoutes(raw)
		if parseErr != nil {
			return fmt.Errorf("parse work routing configuration: %w", parseErr)
		}
		if err := g.EnableWork(k8s, routes); err != nil {
			return fmt.Errorf("configure work event routing: %w", err)
		}
	}
	if issueRoutesFile != "" {
		raw, readErr := os.ReadFile(issueRoutesFile)
		if readErr != nil {
			return fmt.Errorf("read issue routing configuration: %w", readErr)
		}
		routes, parseErr := gateway.ParseIssueRoutes(raw)
		if parseErr != nil {
			return fmt.Errorf("parse issue routing configuration: %w", parseErr)
		}
		if err := g.EnableIssues(k8s, routes); err != nil {
			return fmt.Errorf("configure issue adapter: %w", err)
		}
	}
	internalHandler, err := g.InternalHandler(strings.TrimSpace(string(internalToken)))
	if err != nil {
		return fmt.Errorf("configure internal gateway listener: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	metricServer, err := secureMetricsServer(metricsAddress, certFile, keyFile, config)
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
