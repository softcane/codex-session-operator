/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/controller"
	"github.com/softcane/codex-session-operator/internal/gateway"
	"github.com/softcane/codex-session-operator/internal/observability"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	// +kubebuilder:scaffold:imports
)

func main() {
	if err := run(); err != nil {
		ctrl.Log.WithName("setup").Error(err, "Could not run controller manager")
		os.Exit(1)
	}
}

//nolint:gocyclo
func run() error {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(agentsv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
	setupLog := ctrl.Log.WithName("setup")
	var (
		metricsAddr                                                         string
		metricsCertPath, metricsCertName, metricsCertKey                    string
		webhookCertPath, webhookCertName, webhookCertKey                    string
		webhookPort                                                         int
		enableLeaderElection                                                bool
		probeAddr                                                           string
		secureMetrics                                                       bool
		enableHTTP2                                                         bool
		tlsOpts                                                             []func(*tls.Config)
		agentOwnerScope, executorImages, capabilityImages                   string
		gatewayInternalURL, gatewayInternalTokenFile, gatewayInternalCAFile string
		workspaceStorageClasses, repositoryURLs                             string
	)
	flag.StringVar(&gatewayInternalURL, "gateway-internal-url", os.Getenv("GATEWAY_INTERNAL_URL"),
		"HTTPS lifecycle endpoint of the separate gateway service; empty disables gateway-dependent features.")
	flag.StringVar(&gatewayInternalTokenFile, "gateway-internal-token-file", os.Getenv("GATEWAY_INTERNAL_TOKEN_FILE"),
		"Secret-mounted controller-to-gateway token file.")
	flag.StringVar(&gatewayInternalCAFile, "gateway-internal-ca-file", os.Getenv("GATEWAY_INTERNAL_CA_FILE"),
		"CA certificate for the gateway internal HTTPS endpoint.")
	flag.StringVar(&workspaceStorageClasses, "workspace-storage-classes", os.Getenv("WORKSPACE_STORAGE_CLASSES"),
		"Comma-separated allowlist of StorageClasses for persistent executor workspaces; empty denies all.")
	flag.StringVar(&repositoryURLs, "repository-urls", os.Getenv("REPOSITORY_URLS"),
		"Comma-separated exact HTTPS repository URL allowlist; empty denies repository preparation.")
	flag.StringVar(&executorImages, "executor-images", os.Getenv("EXECUTOR_IMAGES"),
		"Comma-separated allowlist of approved executor image digests; empty denies all.")
	flag.StringVar(&capabilityImages, "capability-images", os.Getenv("CAPABILITY_IMAGES"),
		"Approved capability image digests.")
	flag.StringVar(&agentOwnerScope, "agent-owner-scope", os.Getenv("OPENAI_AGENT_OWNER_SCOPE"),
		"Stable installation/project identity for saved Agent ownership.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.IntVar(&webhookPort, "webhook-port", 9443, "Port the webhook server listens on. "+
		"Defaults to 9443. Set -1 to disable the webhook server.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
		Port:    webhookPort,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/controller/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/controller/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	config, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes configuration: %w", err)
	}
	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&corev1.Secret{}: {Field: fields.OneTermEqualSelector("metadata.name", sandbox.ExecutorSecretName)},
		}},
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "00d2ad21.softcane.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}

	if err := ctrlmetrics.Registry.Register(observability.NewResources(mgr.GetClient())); err != nil {
		return fmt.Errorf("register lifecycle metrics: %w", err)
	}
	openAIClient, err := openaiapi.NewClientWithMetrics(ctrlmetrics.Registry)
	if err != nil {
		return fmt.Errorf("configure OpenAI client: %w", err)
	}
	var messageGateway *gateway.RemoteClient
	if gatewayInternalURL != "" {
		token, tokenErr := os.ReadFile(gatewayInternalTokenFile)
		ca, caErr := os.ReadFile(gatewayInternalCAFile)
		if tokenErr != nil || caErr != nil {
			return fmt.Errorf("read gateway internal credentials: %w", errors.Join(tokenErr, caErr))
		}
		messageGateway, err = gateway.NewRemoteClient(gatewayInternalURL, strings.TrimSpace(string(token)), ca)
		if err != nil {
			return fmt.Errorf("configure gateway client: %w", err)
		}
	}
	connections := &openaiapi.Connections{SDK: openAIClient.SDK}
	if err := mgr.Add(connections); err != nil {
		return fmt.Errorf("add environment observer: %w", err)
	}
	sessionReconciler := &controller.SessionReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		APIReader:               mgr.GetAPIReader(),
		OpenAI:                  openAIClient,
		Agents:                  openAIClient,
		Environments:            openAIClient,
		Configuration:           openAIClient,
		Vaults:                  openAIClient,
		ExecutorImages:          strings.FieldsFunc(executorImages, func(r rune) bool { return r == ',' }),
		CapabilityImages:        strings.FieldsFunc(capabilityImages, func(r rune) bool { return r == ',' }),
		AgentOwnerScope:         agentOwnerScope,
		Connections:             connections,
		WorkspaceStorageClasses: strings.FieldsFunc(workspaceStorageClasses, func(r rune) bool { return r == ',' }),
		RepositoryURLs:          strings.FieldsFunc(repositoryURLs, func(r rune) bool { return r == ',' }),
		ApplicationKeyHash:      sha256.Sum256([]byte(os.Getenv("OPENAI_API_KEY"))),
	}
	if messageGateway != nil {
		sessionReconciler.Gateway = messageGateway
		sessionReconciler.InitialSessions = openAIClient
	}
	if err := sessionReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("create Session controller: %w", err)
	}
	if err := (&controller.AgentReconciler{
		Client:     mgr.GetClient(),
		APIReader:  mgr.GetAPIReader(),
		OpenAI:     openAIClient,
		OwnerScope: agentOwnerScope,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("create Agent controller: %w", err)
	}
	if err := (&controller.EnvironmentReconciler{
		Client:                  mgr.GetClient(),
		APIReader:               mgr.GetAPIReader(),
		OpenAI:                  openAIClient,
		OwnerScope:              agentOwnerScope,
		GatewayEnabled:          messageGateway != nil,
		WorkspaceStorageClasses: strings.FieldsFunc(workspaceStorageClasses, func(r rune) bool { return r == ',' }),
		RepositoryURLs:          strings.FieldsFunc(repositoryURLs, func(r rune) bool { return r == ',' }),
		ExecutorImages:          strings.FieldsFunc(executorImages, func(r rune) bool { return r == ',' }),
		CapabilityImages:        strings.FieldsFunc(capabilityImages, func(r rune) bool { return r == ',' }),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("create Environment controller: %w", err)
	}
	if err := (&controller.VaultReconciler{Client: mgr.GetClient(),
		APIReader:  mgr.GetAPIReader(),
		OpenAI:     openAIClient,
		OwnerScope: agentOwnerScope,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("create Vault controller: %w", err)
	}
	if err := (&controller.VaultCredentialReconciler{Client: mgr.GetClient(),
		APIReader:  mgr.GetAPIReader(),
		OpenAI:     openAIClient,
		OwnerScope: agentOwnerScope,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("create VaultCredential controller: %w", err)
	}
	if err := (&controller.ProfileReconciler{
		Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), OwnerScope: agentOwnerScope,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("create Profile controller: %w", err)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("set up ready check: %w", err)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("run manager: %w", err)
	}
	return nil
}
