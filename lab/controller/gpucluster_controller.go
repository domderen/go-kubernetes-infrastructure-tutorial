package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	infra "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const AllocationFinalizer = "infrastructure.tutorial.example.com/host-cleanup"

type AllocationProvider interface {
	lab.HostAllocator
	ReleaseHosts(context.Context, string) error
}

type GPUClusterReconciler struct {
	client.Client
	Provider AllocationProvider
	// PollInterval controls allocation refreshes after success; nonpositive uses 30s.
	PollInterval time.Duration
}

func (r *GPUClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&infra.GPUCluster{}).
		Complete(r)
}

func (r *GPUClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cluster := &infra.GPUCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	ownerUID := string(cluster.UID)

	// Deletion takes precedence over desired capacity: never provision from
	// this branch. The finalizer keeps the API object available for retries.
	if !cluster.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(cluster, AllocationFinalizer) {
			return ctrl.Result{}, nil
		}
		if err := r.Provider.ReleaseHosts(ctx, ownerUID); err != nil {
			return ctrl.Result{}, fmt.Errorf("release hosts: %w", err)
		}
		// Cleanup is already complete if a prior pass released the hosts and
		// crashed here. Repeating ReleaseHosts must still report success.
		before := cluster.DeepCopy()
		controllerutil.RemoveFinalizer(cluster, AllocationFinalizer)
		err := r.Patch(ctx, cluster, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
		return ctrl.Result{}, err
	}

	if !controllerutil.ContainsFinalizer(cluster, AllocationFinalizer) {
		before := cluster.DeepCopy()
		controllerutil.AddFinalizer(cluster, AllocationFinalizer)
		if err := r.Patch(ctx, cluster, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, fmt.Errorf("persist allocation finalizer: %w", err)
		}
		// The persisted finalizer update queues a new pass through our watch.
		// Start allocation there, after protection is durable in the API server.
		return ctrl.Result{}, nil
	}

	before := cluster.DeepCopy()
	hosts, err := lab.ReconcileHosts(ctx, r.Provider, ownerUID, int(cluster.Spec.NodeCount))
	if err != nil {
		// Keep the last successful capacity report. Failed provider I/O does
		// not prove hosts disappeared or that an ambiguous allocation failed.
		apimeta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type: "Allocated", Status: metav1.ConditionUnknown,
			Reason: "ProviderError", Message: err.Error(),
			ObservedGeneration: cluster.Generation,
		})
		statusErr := r.patchStatus(ctx, before, cluster)
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	cluster.Status.ObservedGeneration = cluster.Generation
	cluster.Status.AllocatedHosts = int32(len(hosts))
	cluster.Status.Hosts = nil
	for _, host := range hosts {
		cluster.Status.Hosts = append(cluster.Status.Hosts, infra.AllocatedHost{ID: host.ID, Slot: int32(host.Slot)})
	}
	apimeta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: "Allocated", Status: metav1.ConditionTrue,
		Reason: "HostsAllocated", Message: "All requested host allocations are present.",
		ObservedGeneration: cluster.Generation,
	})
	if err := r.patchStatus(ctx, before, cluster); err != nil {
		return ctrl.Result{}, err
	}
	// Provider inventory can change without any GPUCluster API update.
	// Periodic passes re-observe it even when status is already converged.
	interval := r.PollInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return ctrl.Result{RequeueAfter: interval}, nil
}

func (r *GPUClusterReconciler) patchStatus(ctx context.Context, before, cluster *infra.GPUCluster) error {
	if equality.Semantic.DeepEqual(before.Status, cluster.Status) {
		return nil
	}
	// Status is a report of the provider's allocations. A failed write can be
	// retried by rediscovering the same stable (UID, slot) allocations.
	return r.Status().Patch(ctx, cluster, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}
