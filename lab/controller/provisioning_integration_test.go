//go:build integration

package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	infra "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/api/v1alpha1"
	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/provisioning"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func TestProvisioningAPIAndManagedRecovery(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Fatal("native envtest assets are required: run make test from lab/")
	}
	env := &envtest.Environment{BinaryAssetsDirectory: assets, CRDDirectoryPaths: []string{filepath.Join("..", "config", "crd", "bases")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := infra.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	api, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{"integrated", "admission"} {
		if err := api.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatal(err)
		}
	}
	cluster := &infra.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: "automatic", Namespace: "integrated"}, Spec: infra.GPUClusterSpec{NodeCount: 2}}
	if err := api.Create(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(cluster)
	cluster = readProvisioningCluster(t, api, key)
	if cluster.Spec.ImageVersion != "os-v1" || cluster.Spec.GPUClass != "mock-a100" || cluster.Spec.MaxParallel != 2 {
		t.Fatalf("API server defaults = %+v", cluster.Spec)
	}
	changed := cluster.DeepCopy()
	changed.Spec.ImageVersion = "os-v2"
	if err := api.Update(t.Context(), changed); !apierrors.IsInvalid(err) {
		t.Fatalf("immutable spec update = %v; want Invalid", err)
	}
	for _, tc := range []struct {
		name string
		spec infra.GPUClusterSpec
	}{
		{"parallel-too-high", infra.GPUClusterSpec{NodeCount: 1, MaxParallel: 11}},
		{"parallel-negative", infra.GPUClusterSpec{NodeCount: 1, MaxParallel: -1}},
		{"nodes-negative", infra.GPUClusterSpec{NodeCount: -1}},
		{"nodes-too-high", infra.GPUClusterSpec{NodeCount: 101}},
	} {
		bad := &infra.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: tc.name, Namespace: "admission"}, Spec: tc.spec}
		if err := api.Create(t.Context(), bad); !apierrors.IsInvalid(err) {
			t.Fatalf("invalid %s admitted: %v", tc.name, err)
		}
	}
	unsupported := &infra.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: "unsupported", Namespace: "admission"}, Spec: infra.GPUClusterSpec{NodeCount: 0, GPUClass: "unsupported-class"}}
	if err := api.Create(t.Context(), unsupported); err != nil {
		t.Fatalf("unsupported GPU class must reach workflow validation: %v", err)
	}
	t.Run("deletion retains original operation after status loss", func(t *testing.T) {
		cluster := &infra.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: "delete-running", Namespace: "admission"}, Spec: infra.GPUClusterSpec{NodeCount: 1}}
		if err := api.Create(t.Context(), cluster); err != nil {
			t.Fatal(err)
		}
		key := client.ObjectKeyFromObject(cluster)
		provider := &provisioningInventory{SimulatedAllocator: &lab.SimulatedAllocator{}}
		ops := &recordedOperations{states: map[string]OperationState{}, onStart: func(ctx context.Context, req provisioning.Request) OperationState {
			if _, err := lab.ReconcileHosts(ctx, provider, req.ClusterUID, req.NodeCount); err != nil {
				t.Fatal(err)
			}
			return OperationState{Phase: PhaseProvisioning, RunID: "stopped-worker-run"}
		}}
		r := &ProvisioningReconciler{Client: api, Provider: provider, Operations: ops}
		provisioningReconcileOK(t, r, key)
		provisioningReconcileOK(t, r, key)
		before := readProvisioningCluster(t, api, key)
		originalID := ops.ids[0]
		if before.Annotations[WorkflowIDAnnotation] != originalID {
			t.Fatalf("operation identity was not persisted before deletion: %v", before.Annotations)
		}
		before.Status = infra.GPUClusterStatus{}
		if err := api.Status().Update(t.Context(), before); err != nil {
			t.Fatal(err)
		}
		if err := api.Delete(t.Context(), before); err != nil {
			t.Fatal(err)
		}
		deleting := readProvisioningCluster(t, api, key)
		if deleting.Generation <= before.Generation {
			t.Fatalf("expected real deletion generation bump: before=%d after=%d", before.Generation, deleting.Generation)
		}
		provisioningReconcileOK(t, r, key)
		deleting = readProvisioningCluster(t, api, key)
		if !controllerutil.ContainsFinalizer(deleting, AllocationFinalizer) || len(ops.cancelIDs) != 1 || ops.cancelIDs[0] != originalID {
			t.Fatalf("deletion failed to await original running operation: finalizers=%v cancelIDs=%v original=%s", deleting.Finalizers, ops.cancelIDs, originalID)
		}
		if hosts, err := provider.ListHosts(t.Context(), string(before.UID)); err != nil || len(hosts) != 0 {
			t.Fatalf("running workflow deletion did not fence/release hosts: %v, %v", hosts, err)
		}
		ops.states[originalID] = OperationState{Phase: PhaseFailed, RunID: "stopped-worker-run", LastError: "canceled after worker restart"}
		provisioningReconcileOK(t, r, key)
		if err := api.Get(t.Context(), key, &infra.GPUCluster{}); !apierrors.IsNotFound(err) {
			t.Fatalf("terminal original operation retained finalizer: %v", err)
		}
	})

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme, Logger: logr.Discard(),
		Cache:   cache.Options{DefaultNamespaces: map[string]cache.Config{"integrated": {}}},
		Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := &lab.SimulatedAllocator{}
	ops := &managedProvisioningOperations{provider: provider, api: api, key: key, states: map[string]OperationState{}}
	r := &ProvisioningReconciler{Client: mgr.GetClient(), Provider: provider, Operations: ops, PollInterval: time.Hour}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	managerCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(managerCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("integrated manager shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("integrated manager failed to stop")
		}
	})
	waitForProvisioningReady(t, api, key)
	before := readProvisioningCluster(t, api, key)
	if before.Status.WorkflowID != ProvisioningWorkflowID(string(before.UID), before.Generation) || before.Status.RunID != "retained-run" || len(before.Status.Hosts) != 2 {
		t.Fatalf("API server lost integrated status fields: %+v", before.Status)
	}
	cluster = before.DeepCopy()
	cluster.Status = infra.GPUClusterStatus{}
	if err := api.Status().Update(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	// One-hour poll cannot repair this test within ten seconds. The repair
	// proves our manager also watches status-only changes at the same generation.
	waitForProvisioningReady(t, api, key)
	repaired := readProvisioningCluster(t, api, key)
	if repaired.Generation != before.Generation || repaired.Status.RunID != before.Status.RunID || ops.startCount() != 1 {
		t.Fatalf("status-only repair restarted durable operation: %+v; starts=%d", repaired.Status, ops.startCount())
	}
	if err := api.Delete(t.Context(), repaired); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(t.Context(), 10*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		err := api.Get(ctx, key, &infra.GPUCluster{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}); err != nil {
		t.Fatalf("integrated cleanup: %v", err)
	}
	if hosts, err := provider.ListHosts(t.Context(), string(before.UID)); err != nil || len(hosts) != 0 {
		t.Fatalf("deletion left provider inventory: %v, %v", hosts, err)
	}
	if _, err := provider.EnsureHost(t.Context(), string(before.UID), 1); !errors.Is(err, lab.ErrOwnerReleased) {
		t.Fatalf("deleted owner permits late allocation: %v", err)
	}
}

func waitForProvisioningReady(t *testing.T, api client.Client, key client.ObjectKey) {
	t.Helper()
	var last infra.GPUClusterStatus
	err := wait.PollUntilContextTimeout(t.Context(), 10*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		cluster := &infra.GPUCluster{}
		if err := api.Get(ctx, key, cluster); err != nil {
			return false, err
		}
		last = cluster.Status
		return last.Phase == PhaseReady && last.ReadyHosts == 2 && apimeta.IsStatusConditionTrue(last.Conditions, "Ready"), nil
	})
	if err != nil {
		t.Fatalf("integrated ready wait: %v; last=%+v", err, last)
	}
}

// Represents the Temporal boundary while envtest exercises real Kubernetes
// persistence, admission, watches, and status/finalizer conflict behavior.
type managedProvisioningOperations struct {
	mu       sync.Mutex
	provider *lab.SimulatedAllocator
	api      client.Client
	key      client.ObjectKey
	states   map[string]OperationState
	starts   int
}

func (o *managedProvisioningOperations) Ensure(ctx context.Context, id string, req provisioning.Request) (OperationState, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if state, ok := o.states[id]; ok {
		return state, nil
	}
	cluster := &infra.GPUCluster{}
	if err := o.api.Get(ctx, o.key, cluster); err != nil {
		return OperationState{}, err
	}
	if !controllerutil.ContainsFinalizer(cluster, AllocationFinalizer) || cluster.Annotations[WorkflowIDAnnotation] != id {
		return OperationState{}, fmt.Errorf("workflow dispatch lacks durable identity and finalizer")
	}
	hosts, err := lab.ReconcileHosts(ctx, o.provider, req.ClusterUID, req.NodeCount)
	if err != nil {
		return OperationState{}, err
	}
	o.starts++
	state := OperationState{Phase: PhaseReady, RunID: "retained-run", Hosts: hosts}
	o.states[id] = state
	return state, nil
}

func (o *managedProvisioningOperations) Observe(_ context.Context, id string) (OperationState, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if state, ok := o.states[id]; ok {
		return state, nil
	}
	return OperationState{}, ErrOperationNotFound
}

func (o *managedProvisioningOperations) Cancel(_ context.Context, id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.states[id]; !ok {
		return ErrOperationNotFound
	}
	o.states[id] = OperationState{Phase: PhaseFailed, RunID: "retained-run", LastError: "canceled"}
	return nil
}

func (o *managedProvisioningOperations) startCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.starts
}
