package main

import (
	"flag"
	"log/slog"
	"os"
	"time"

	infra "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/api/v1alpha1"
	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/controller"
	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/mockprovider"
	"go.temporal.io/sdk/client"
	temporallog "go.temporal.io/sdk/log"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func main() {
	// controller-runtime registers kubeconfig on the global FlagSet. This CLI
	// owns a separate set so its explicit local-cluster flags cannot collide.
	flags := flag.NewFlagSet("controller", flag.ExitOnError)
	kubeconfig := flags.String("kubeconfig", "../.local/kubeconfig", "dedicated lab kubeconfig")
	kubecontext := flags.String("context", "kind-infrastructure-tutorial", "explicit Kubernetes context")
	address := flags.String("temporal-address", "127.0.0.1:7233", "Temporal frontend")
	namespace := flags.String("temporal-namespace", "default", "Temporal namespace")
	queue := flags.String("task-queue", "infrastructure-tutorial", "dedicated Temporal task queue")
	providerURL := flags.String("provider-url", "http://127.0.0.1:9080", "simulated provider endpoint")
	_ = flags.Parse(os.Args[1:])
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: *kubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: *kubecontext},
	).ClientConfig()
	if err != nil {
		fatal("load lab kubeconfig", err)
	}
	scheme := runtime.NewScheme()
	if err := infra.AddToScheme(scheme); err != nil {
		fatal("register GPUCluster", err)
	}
	mgr, err := ctrl.NewManager(config, ctrl.Options{Scheme: scheme, Metrics: server.Options{BindAddress: "0"}, HealthProbeBindAddress: "127.0.0.1:8091"})
	if err != nil {
		fatal("create controller manager", err)
	}
	temporal, err := client.Dial(client.Options{HostPort: *address, Namespace: *namespace, Logger: temporallog.NewStructuredLogger(slog.Default())})
	if err != nil {
		fatal("connect Temporal", err)
	}
	defer temporal.Close()
	reconciler := &controller.ProvisioningReconciler{
		Client: mgr.GetClient(), Provider: mockprovider.NewClient(*providerURL),
		Operations: controller.NewTemporalOperations(temporal, *queue), PollInterval: time.Second,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		fatal("register provisioning controller", err)
	}
	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		fatal("register health check", err)
	}
	if err := mgr.AddReadyzCheck("ping", healthz.Ping); err != nil {
		fatal("register readiness check", err)
	}
	slog.Info("GPUCluster controller starting", "context", *kubecontext)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		fatal("controller stopped", err)
	}
}

func fatal(message string, err error) {
	slog.Error(message, "error", err)
	os.Exit(1)
}
