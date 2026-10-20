package main

import (
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/mockprovider"
	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/provisioning"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporallog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
)

func main() {
	address := flag.String("temporal-address", "127.0.0.1:7233", "Temporal frontend address")
	namespace := flag.String("temporal-namespace", "default", "Temporal namespace")
	queue := flag.String("task-queue", "infrastructure-tutorial", "dedicated task queue")
	providerURL := flag.String("provider-url", "http://127.0.0.1:9080", "simulated provider endpoint")
	flag.Parse()
	c, err := client.Dial(client.Options{HostPort: *address, Namespace: *namespace, Logger: temporallog.NewStructuredLogger(slog.Default())})
	if err != nil {
		slog.Error("connect Temporal", "error", err)
		os.Exit(1)
	}
	defer c.Close()
	provider := mockprovider.NewClient(*providerURL)
	w := worker.New(c, *queue, worker.Options{MaxConcurrentActivityExecutionSize: 8, WorkerStopTimeout: 5 * time.Second})
	w.RegisterWorkflow(provisioning.ProvisionCluster)
	reservation := &provisioning.Activities{Provider: provider}
	imaging := &provisioning.ImagingActivities{Provider: provider, PollInterval: 500 * time.Millisecond}
	lifecycle := &provisioning.LifecycleActivities{Provider: provider}
	w.RegisterActivityWithOptions(reservation.ReserveHosts, activity.RegisterOptions{Name: provisioning.ReserveHostsActivity})
	w.RegisterActivityWithOptions(imaging.EnsureImage, activity.RegisterOptions{Name: provisioning.EnsureImageActivity})
	w.RegisterActivityWithOptions(lifecycle.JoinCluster, activity.RegisterOptions{Name: provisioning.JoinClusterActivity})
	w.RegisterActivityWithOptions(lifecycle.ValidateGPU, activity.RegisterOptions{Name: provisioning.ValidateGPUActivity})
	w.RegisterActivityWithOptions(lifecycle.ReleaseHosts, activity.RegisterOptions{Name: provisioning.ReleaseHostsActivity})
	slog.Info("provisioning worker starting", "taskQueue", *queue, "provider", *providerURL)
	if err := w.Run(worker.InterruptCh()); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
