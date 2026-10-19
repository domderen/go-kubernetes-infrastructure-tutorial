//go:build integration

package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	infra "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/api/v1alpha1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Exercises our watch registration against envtest. This test only changes API
// objects: the manager invokes Reconcile, rather than the test calling it.
func testManagedLifecycle(t *testing.T, cfg *rest.Config, scheme *runtime.Scheme, api client.Client) {
	t.Helper()
	ctx := t.Context()
	namespace := "managed"
	if err := api.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil {
		t.Fatal(err)
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Logger: logr.Discard(),
		// Keep this manager away from the direct-call tests' objects/providers.
		Cache:   cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}},
		Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := &lab.SimulatedAllocator{}
	r := &GPUClusterReconciler{Client: mgr.GetClient(), Provider: provider, PollInterval: time.Hour}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	managerCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(managerCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("manager shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("manager did not finish after cancellation")
		}
	})

	cluster := &infra.GPUCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "automatic", Namespace: namespace},
		Spec:       infra.GPUClusterSpec{NodeCount: 2},
	}
	if err := api.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(cluster)
	waitForManagedAllocation(t, api, key)
	before := assertHostCount(t, provider, string(cluster.UID), 2)

	// A status-only API update does not change generation. The watch still
	// causes our controller to repair its report from provider observations.
	cluster = getCluster(t, api, key)
	cluster.Status.AllocatedHosts = 0
	cluster.Status.Hosts = nil
	cluster.Status.Conditions = nil
	if err := api.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	waitForManagedAllocation(t, api, key)
	if after := assertHostCount(t, provider, string(cluster.UID), 2); !reflect.DeepEqual(before, after) {
		t.Fatal("watch-driven status repair changed existing allocations")
	}

	if err := api.Delete(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	err = wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		err := api.Get(ctx, key, &infra.GPUCluster{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
	if err != nil {
		t.Fatalf("automatic cleanup did not finish: %v", err)
	}
	assertHostCount(t, provider, string(cluster.UID), 0)
}

func waitForManagedAllocation(t *testing.T, api client.Client, key client.ObjectKey) {
	t.Helper()
	var last infra.GPUClusterStatus
	err := wait.PollUntilContextTimeout(t.Context(), 10*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		cluster := &infra.GPUCluster{}
		if err := api.Get(ctx, key, cluster); err != nil {
			return false, err
		}
		last = cluster.Status
		return last.AllocatedHosts == 2 && len(last.Hosts) == 2 && apimeta.IsStatusConditionTrue(last.Conditions, "Allocated"), nil
	})
	if err != nil {
		t.Fatalf("manager did not confirm allocations: %v; last status: %+v", err, last)
	}
}
