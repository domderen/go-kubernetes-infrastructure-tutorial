//go:build integration

package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	infra "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestGPUClusterLifecycle(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Fatal("native envtest assets are required: run make test from lab/")
	}
	env := &envtest.Environment{
		BinaryAssetsDirectory: assets,
		CRDDirectoryPaths:     []string{filepath.Join("..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
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
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "lesson"}}); err != nil {
		t.Fatal(err)
	}

	// Catches allocating before protection is persisted, duplicate allocations,
	// and status writes that enqueue more work despite unchanged observations.
	t.Run("protection then allocation and converged no-op", func(t *testing.T) {
		cluster := createCluster(t, c, "allocate")
		key := client.ObjectKeyFromObject(cluster)
		provider := &protectedProvider{SimulatedAllocator: &lab.SimulatedAllocator{}, client: c, key: key}
		r := &GPUClusterReconciler{Client: c, Provider: provider}
		reconcileOK(t, r, key)
		cluster = getCluster(t, c, key)
		if !controllerutil.ContainsFinalizer(cluster, AllocationFinalizer) {
			t.Fatal("first pass did not persist allocation protection")
		}
		assertHostCount(t, provider, string(cluster.UID), 0)
		reconcileOK(t, r, key)
		cluster = getCluster(t, c, key)
		assertHostCount(t, provider, string(cluster.UID), 2)
		if cluster.Status.AllocatedHosts != 2 || len(cluster.Status.Hosts) != 2 || cluster.Status.ObservedGeneration != cluster.Generation {
			t.Fatalf("allocation status = %+v; want two hosts for generation %d", cluster.Status, cluster.Generation)
		}
		before := cluster.DeepCopy()
		reconcileOK(t, r, key)
		cluster = getCluster(t, c, key)
		if cluster.ResourceVersion != before.ResourceVersion || !reflect.DeepEqual(cluster.Status, before.Status) {
			t.Fatal("converged reconciliation wrote status again")
		}
	})

	// Catches ignoring a real API conflict and then allocating without the
	// finalizer, or overwriting another writer's newly added finalizer.
	t.Run("finalizer conflict prevents allocation", func(t *testing.T) {
		cluster := createCluster(t, c, "protect-conflict")
		key := client.ObjectKeyFromObject(cluster)
		provider := &lab.SimulatedAllocator{}
		otherFinalizer := "other.example.com/cleanup"
		conflicting := &faultClient{Client: c, beforePatch: func(ctx context.Context, obj client.Object) error {
			fresh := getCluster(t, c, key)
			controllerutil.AddFinalizer(fresh, otherFinalizer)
			return c.Update(ctx, fresh)
		}}
		r := &GPUClusterReconciler{Client: conflicting, Provider: provider}
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		if !apierrors.IsConflict(err) {
			t.Fatalf("finalizer write error = %v; want conflict", err)
		}
		assertHostCount(t, provider, string(cluster.UID), 0)
		r.Client = c
		reconcileOK(t, r, key)
		fresh := getCluster(t, c, key)
		if !controllerutil.ContainsFinalizer(fresh, otherFinalizer) || !controllerutil.ContainsFinalizer(fresh, AllocationFinalizer) {
			t.Fatalf("retry lost a finalizer: %v", fresh.Finalizers)
		}
	})

	// Catches removing protection after failed cleanup, allocating during
	// deletion, deleting other owners' hosts, or removing others' finalizers.
	t.Run("cleanup failure retains protection", func(t *testing.T) {
		cluster := createCluster(t, c, "cleanup-retry")
		key := client.ObjectKeyFromObject(cluster)
		provider := &protectedProvider{SimulatedAllocator: &lab.SimulatedAllocator{}, client: c, key: key}
		r := &GPUClusterReconciler{Client: c, Provider: provider}
		reconcileOK(t, r, key)
		reconcileOK(t, r, key)
		cluster = getCluster(t, c, key)
		otherFinalizer := "other.example.com/cleanup"
		controllerutil.AddFinalizer(cluster, otherFinalizer)
		if err := c.Update(ctx, cluster); err != nil {
			t.Fatal(err)
		}
		if _, err := provider.SimulatedAllocator.EnsureHost(ctx, "unrelated-uid", 1); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete(ctx, cluster); err != nil {
			t.Fatal(err)
		}
		provider.releaseErr = errors.New("provider unavailable")
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); !errors.Is(err, provider.releaseErr) {
			t.Fatalf("cleanup error = %v; want provider unavailable", err)
		}
		cluster = getCluster(t, c, key)
		if cluster.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(cluster, AllocationFinalizer) {
			t.Fatal("failed cleanup lost pending-deletion protection")
		}
		assertHostCount(t, provider, string(cluster.UID), 2)
		provider.releaseErr = nil
		reconcileOK(t, r, key)
		cluster = getCluster(t, c, key)
		if controllerutil.ContainsFinalizer(cluster, AllocationFinalizer) || !controllerutil.ContainsFinalizer(cluster, otherFinalizer) {
			t.Fatalf("successful cleanup removed wrong keys: %v", cluster.Finalizers)
		}
		assertHostCount(t, provider, string(cluster.UID), 0)
		assertHostCount(t, provider, "unrelated-uid", 1)
	})

	// Catches treating already-released hosts as failure after cleanup succeeds
	// but its finalizer removal is not saved. A new reconciler has no memory.
	t.Run("cleanup succeeds but finalizer write is lost", func(t *testing.T) {
		cluster := createCluster(t, c, "cleanup-lost-write")
		key := client.ObjectKeyFromObject(cluster)
		provider := &lab.SimulatedAllocator{}
		r := &GPUClusterReconciler{Client: c, Provider: provider}
		reconcileOK(t, r, key)
		reconcileOK(t, r, key)
		cluster = getCluster(t, c, key)
		if err := c.Delete(ctx, cluster); err != nil {
			t.Fatal(err)
		}
		lostWrite := errors.New("finalizer write unavailable")
		r.Client = &faultClient{Client: c, beforePatch: func(context.Context, client.Object) error { return lostWrite }}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); !errors.Is(err, lostWrite) {
			t.Fatalf("finalizer removal error = %v; want injected failure", err)
		}
		assertHostCount(t, provider, string(cluster.UID), 0)
		if !controllerutil.ContainsFinalizer(getCluster(t, c, key), AllocationFinalizer) {
			t.Fatal("failed write unexpectedly removed finalizer")
		}
		r = &GPUClusterReconciler{Client: c, Provider: provider}
		reconcileOK(t, r, key)
		if err := c.Get(ctx, key, &infra.GPUCluster{}); !apierrors.IsNotFound(err) {
			t.Fatalf("object after finalizer removal = %v; want NotFound", err)
		}
		reconcileOK(t, r, key)
		if _, err := provider.EnsureHost(ctx, string(cluster.UID), 1); !errors.Is(err, lab.ErrOwnerReleased) {
			t.Fatalf("late allocation error = %v; want ErrOwnerReleased", err)
		}
	})

	// Catches using last-reported status as the source of allocation identity.
	t.Run("allocation survives lost status write", func(t *testing.T) {
		cluster := createCluster(t, c, "status-lost-write")
		key := client.ObjectKeyFromObject(cluster)
		provider := &lab.SimulatedAllocator{}
		r := &GPUClusterReconciler{Client: c, Provider: provider}
		reconcileOK(t, r, key)
		lostWrite := errors.New("status write unavailable")
		r.Client = &faultClient{Client: c, statusErr: lostWrite}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); !errors.Is(err, lostWrite) {
			t.Fatalf("status write error = %v; want injected failure", err)
		}
		before := assertHostCount(t, provider, string(cluster.UID), 2)
		if getCluster(t, c, key).Status.AllocatedHosts != 0 {
			t.Fatal("injected failed write unexpectedly saved status")
		}
		r = &GPUClusterReconciler{Client: c, Provider: provider}
		reconcileOK(t, r, key)
		after := assertHostCount(t, provider, string(cluster.UID), 2)
		if !reflect.DeepEqual(before, after) || getCluster(t, c, key).Status.AllocatedHosts != 2 {
			t.Fatal("retry did not report the same two existing allocations")
		}
	})

	// Catches falsely confirming allocations after a failed observation,
	// discarding last-known inventory, rewriting an unchanged failure, and
	// failing to confirm the existing hosts again when the provider recovers.
	t.Run("provider outage reports uncertainty and recovers", func(t *testing.T) {
		cluster := createCluster(t, c, "provider-outage")
		key := client.ObjectKeyFromObject(cluster)
		provider := &observedProvider{SimulatedAllocator: &lab.SimulatedAllocator{}}
		r := &GPUClusterReconciler{Client: c, Provider: provider}
		reconcileOK(t, r, key)
		reconcileOK(t, r, key)
		cluster = getCluster(t, c, key)
		// This condition belongs to a different writer and must be preserved.
		cluster.Status.Conditions = append(cluster.Status.Conditions, metav1.Condition{
			Type: "ExternalAudit", Status: metav1.ConditionTrue, Reason: "AuditPassed",
			Message: "Inventory audit passed", ObservedGeneration: cluster.Generation,
			LastTransitionTime: metav1.Now(),
		})
		if err := c.Status().Update(ctx, cluster); err != nil {
			t.Fatal(err)
		}
		before := getCluster(t, c, key).DeepCopy()
		provider.listErr = errors.New("provider request timed out")
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); !errors.Is(err, provider.listErr) {
			t.Fatalf("reconcile error = %v; want provider error for retry", err)
		}
		cluster = getCluster(t, c, key)
		condition := apimeta.FindStatusCondition(cluster.Status.Conditions, "Allocated")
		if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "ProviderError" {
			t.Fatalf("allocation condition = %+v; want Unknown / ProviderError", condition)
		}
		if condition.ObservedGeneration != cluster.Generation || condition.LastTransitionTime.IsZero() || !strings.Contains(condition.Message, "provider request timed out") {
			t.Fatalf("condition lacks failure context: %+v", condition)
		}
		if cluster.Status.AllocatedHosts != 2 || cluster.Status.ObservedGeneration != before.Status.ObservedGeneration || !reflect.DeepEqual(cluster.Status.Hosts, before.Status.Hosts) {
			t.Fatalf("provider outage changed last-known inventory: %+v", cluster.Status)
		}
		if !reflect.DeepEqual(apimeta.FindStatusCondition(cluster.Status.Conditions, "ExternalAudit"), apimeta.FindStatusCondition(before.Status.Conditions, "ExternalAudit")) {
			t.Fatal("updating allocation condition changed another writer's condition")
		}
		failedVersion := cluster.ResourceVersion
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); !errors.Is(err, provider.listErr) {
			t.Fatalf("repeated outage error = %v; want provider error", err)
		}
		if getCluster(t, c, key).ResourceVersion != failedVersion {
			t.Fatal("unchanged failure rewrote status or transition time")
		}
		provider.listErr = nil
		reconcileOK(t, r, key)
		cluster = getCluster(t, c, key)
		condition = apimeta.FindStatusCondition(cluster.Status.Conditions, "Allocated")
		if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "HostsAllocated" {
			t.Fatalf("recovered condition = %+v; want True / HostsAllocated", condition)
		}
		hosts := assertHostCount(t, provider, string(cluster.UID), 2)
		for i, host := range hosts {
			if host.ID != before.Status.Hosts[i].ID {
				t.Fatal("provider recovery allocated replacement hosts")
			}
		}
	})

	// Catches losing the retryable provider cause when saving Unknown also
	// fails, or hiding the API failure that prevented the condition appearing.
	t.Run("provider and condition-write failures are both returned", func(t *testing.T) {
		cluster := createCluster(t, c, "condition-write-failure")
		key := client.ObjectKeyFromObject(cluster)
		provider := &observedProvider{SimulatedAllocator: &lab.SimulatedAllocator{}}
		r := &GPUClusterReconciler{Client: c, Provider: provider}
		reconcileOK(t, r, key)
		reconcileOK(t, r, key)
		provider.listErr = errors.New("provider unavailable")
		writeErr := errors.New("condition write unavailable")
		r.Client = &faultClient{Client: c, statusErr: writeErr}
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		if !errors.Is(err, provider.listErr) || !errors.Is(err, writeErr) {
			t.Fatalf("reconcile error = %v; want both provider and API causes", err)
		}
	})

	// Catches an already-converged object never being checked again unless
	// Kubernetes changes, and ignoring the configured provider poll interval.
	t.Run("successful passes schedule provider polls", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			interval time.Duration
			want     time.Duration
		}{
			{name: "default", want: 30 * time.Second},
			{name: "configured", interval: 17 * time.Second, want: 17 * time.Second},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cluster := createCluster(t, c, "poll-"+tc.name)
				key := client.ObjectKeyFromObject(cluster)
				r := &GPUClusterReconciler{Client: c, Provider: &lab.SimulatedAllocator{}, PollInterval: tc.interval}
				reconcileOK(t, r, key)
				// Check both the pass that writes status and a converged no-op.
				for pass := range 2 {
					result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
					if err != nil || result.RequeueAfter != tc.want {
						t.Fatalf("pass %d returned %v, %v; want poll after %s", pass, result, err, tc.want)
					}
				}
			})
		}
	})

	t.Run("manager reacts to API changes", func(t *testing.T) {
		testManagedLifecycle(t, cfg, scheme, c)
	})

	// Catches a schema that accepts invalid input or permits spec changes while
	// this controller has no revision/scaling policy.
	t.Run("API rejects invalid counts and spec changes", func(t *testing.T) {
		cluster := &infra.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: "lesson"}, Spec: infra.GPUClusterSpec{NodeCount: -1}}
		if err := c.Create(ctx, cluster); !apierrors.IsInvalid(err) {
			t.Fatalf("negative count error = %v; want Invalid", err)
		}
		cluster = createCluster(t, c, "immutable")
		cluster.Spec.NodeCount = 3
		if err := c.Update(ctx, cluster); !apierrors.IsInvalid(err) {
			t.Fatalf("spec change error = %v; want Invalid", err)
		}
	})
}

func createCluster(t *testing.T, c client.Client, name string) *infra.GPUCluster {
	t.Helper()
	cluster := &infra.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "lesson"}, Spec: infra.GPUClusterSpec{NodeCount: 2}}
	if err := c.Create(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	return cluster
}

func getCluster(t *testing.T, c client.Client, key client.ObjectKey) *infra.GPUCluster {
	t.Helper()
	cluster := &infra.GPUCluster{}
	if err := c.Get(t.Context(), key, cluster); err != nil {
		t.Fatal(err)
	}
	return cluster
}

func reconcileOK(t *testing.T, r *GPUClusterReconciler, key client.ObjectKey) {
	t.Helper()
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
}

func assertHostCount(t *testing.T, p lab.HostAllocator, uid string, want int) []lab.HostAllocation {
	t.Helper()
	hosts, err := p.ListHosts(t.Context(), uid)
	if err != nil || len(hosts) != want {
		t.Fatalf("inventory for %s = %v, %v; want %d hosts", uid, hosts, err, want)
	}
	return hosts
}

type protectedProvider struct {
	*lab.SimulatedAllocator
	client     client.Client
	key        client.ObjectKey
	releaseErr error
}

// Discovery alone is faulted; allocation, cleanup, and recovery use the real
// simulator so the test can verify that existing host identities are retained.
type observedProvider struct {
	*lab.SimulatedAllocator
	listErr error
}

func (p *observedProvider) ListHosts(ctx context.Context, uid string) ([]lab.HostAllocation, error) {
	if p.listErr != nil {
		return nil, p.listErr
	}
	return p.SimulatedAllocator.ListHosts(ctx, uid)
}

func (p *protectedProvider) EnsureHost(ctx context.Context, uid string, slot int) (lab.HostAllocation, error) {
	cluster := &infra.GPUCluster{}
	if err := p.client.Get(ctx, p.key, cluster); err != nil {
		return lab.HostAllocation{}, err
	}
	if !controllerutil.ContainsFinalizer(cluster, AllocationFinalizer) {
		return lab.HostAllocation{}, errors.New("allocation attempted without a persisted finalizer")
	}
	return p.SimulatedAllocator.EnsureHost(ctx, uid, slot)
}

func (p *protectedProvider) ReleaseHosts(ctx context.Context, uid string) error {
	if p.releaseErr != nil {
		return p.releaseErr
	}
	return p.SimulatedAllocator.ReleaseHosts(ctx, uid)
}

// Faults replace only API writes; all reads and successful writes still reach
// envtest, including resourceVersion conflict checks and deletion semantics.
type faultClient struct {
	client.Client
	beforePatch func(context.Context, client.Object) error
	statusErr   error
}

func (c *faultClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if c.beforePatch != nil {
		if err := c.beforePatch(ctx, obj); err != nil {
			return err
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *faultClient) Status() client.SubResourceWriter {
	return &faultStatusWriter{SubResourceWriter: c.Client.Status(), err: c.statusErr}
}

type faultStatusWriter struct {
	client.SubResourceWriter
	err error
}

func (w *faultStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if w.err != nil {
		return w.err
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}
