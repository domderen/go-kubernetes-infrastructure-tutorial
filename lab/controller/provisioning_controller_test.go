package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	infra "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/api/v1alpha1"
	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/provisioning"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestProvisioningFinalizerBeforeDispatch(t *testing.T) {
	c, key, cluster := provisioningClient(t, false)
	provider := &provisioningInventory{SimulatedAllocator: &lab.SimulatedAllocator{}}
	ops := &recordedOperations{states: map[string]OperationState{}, onStart: func(ctx context.Context, req provisioning.Request) OperationState {
		fresh := readProvisioningCluster(t, c, key)
		if !controllerutil.ContainsFinalizer(fresh, AllocationFinalizer) || fresh.Annotations[WorkflowIDAnnotation] != req.OperationID {
			t.Fatal("workflow dispatched before durable identity and finalizer")
		}
		return OperationState{Phase: PhaseProvisioning, RunID: "run-1"}
	}}
	r := &ProvisioningReconciler{Client: c, Provider: provider, Operations: ops}
	provisioningReconcileOK(t, r, key)
	fresh := readProvisioningCluster(t, c, key)
	if len(ops.requests) != 0 || !controllerutil.ContainsFinalizer(fresh, AllocationFinalizer) || fresh.Annotations[WorkflowIDAnnotation] != ProvisioningWorkflowID(string(cluster.UID), cluster.Generation) {
		t.Fatal("first reconcile must only persist identity and finalizer")
	}
	result := provisioningReconcileOK(t, r, key)
	if result.RequeueAfter != 2*time.Second {
		t.Fatalf("successful poll interval = %v; want 2s", result.RequeueAfter)
	}
	wantID := ProvisioningWorkflowID(string(cluster.UID), cluster.Generation)
	if ops.ids[0] != wantID || ops.requests[0].OperationID != wantID || ops.requests[0].ClusterUID != string(cluster.UID) || ops.requests[0].NodeCount != 2 {
		t.Fatalf("unstable dispatch: ids=%v request=%+v", ops.ids, ops.requests[0])
	}
	if got := ops.requests[0]; got.ImageVersion != "os-v1" || got.GPUClass != "mock-a100" || got.MaxParallel != 2 {
		t.Fatalf("request defaults = %+v", got)
	}
	got := readProvisioningCluster(t, c, key)
	if got.Status.WorkflowID != wantID || got.Status.RunID != "run-1" || got.Status.Phase != PhaseProvisioning || apimeta.IsStatusConditionTrue(got.Status.Conditions, "Ready") {
		t.Fatalf("provisioning status = %+v", got.Status)
	}
	if ProvisioningWorkflowID("different-uid", cluster.Generation) == wantID || ProvisioningWorkflowID(string(cluster.UID), cluster.Generation+1) == wantID {
		t.Fatal("operation identity must include UID and generation")
	}
}

func TestProvisioningFailedFinalizerWritePreventsDispatch(t *testing.T) {
	c, key, _ := provisioningClient(t, false)
	lost := errors.New("finalizer conflict")
	ops := &recordedOperations{states: map[string]OperationState{}}
	r := &ProvisioningReconciler{Client: &provisioningPatchFault{Client: c, err: lost}, Provider: &provisioningInventory{SimulatedAllocator: &lab.SimulatedAllocator{}}, Operations: ops}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); !errors.Is(err, lost) {
		t.Fatalf("finalizer write error = %v", err)
	}
	if len(ops.requests) != 0 || controllerutil.ContainsFinalizer(readProvisioningCluster(t, c, key), AllocationFinalizer) {
		t.Fatal("failed finalizer write allowed workflow dispatch")
	}
}

func TestProvisioningRecoversCompletedOperationAfterLostStatus(t *testing.T) {
	c, key, cluster := provisioningClient(t, true)
	provider := &provisioningInventory{SimulatedAllocator: &lab.SimulatedAllocator{}}
	ops := &recordedOperations{states: map[string]OperationState{}, onStart: func(ctx context.Context, req provisioning.Request) OperationState {
		hosts, err := lab.ReconcileHosts(ctx, provider.SimulatedAllocator, req.ClusterUID, req.NodeCount)
		if err != nil {
			t.Fatal(err)
		}
		return OperationState{Phase: PhaseReady, RunID: "completed-run", Hosts: hosts}
	}}
	lost := errors.New("status response lost")
	r := &ProvisioningReconciler{Client: &provisioningStatusFault{Client: c, err: lost}, Provider: provider, Operations: ops}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); !errors.Is(err, lost) {
		t.Fatalf("status write error = %v", err)
	}
	if readProvisioningCluster(t, c, key).Status.WorkflowID != "" || ops.starts != 1 {
		t.Fatal("test did not lose status after completed dispatch")
	}
	r = &ProvisioningReconciler{Client: c, Provider: provider, Operations: ops, PollInterval: time.Minute}
	result := provisioningReconcileOK(t, r, key)
	got := readProvisioningCluster(t, c, key)
	if result.RequeueAfter != time.Minute || got.Status.RunID != "completed-run" || got.Status.Phase != PhaseReady || got.Status.ReadyHosts != 2 || got.Status.AllocatedHosts != 2 || len(got.Status.Hosts) != 2 || got.Status.ObservedGeneration != cluster.Generation {
		t.Fatalf("recovered status = %+v; result=%+v", got.Status, result)
	}
	if !apimeta.IsStatusConditionTrue(got.Status.Conditions, "Ready") || !apimeta.IsStatusConditionTrue(got.Status.Conditions, "Allocated") {
		t.Fatalf("completed conditions = %+v", got.Status.Conditions)
	}
	before := got.DeepCopy()
	provisioningReconcileOK(t, r, key)
	if ops.starts != 1 || readProvisioningCluster(t, c, key).ResourceVersion != before.ResourceVersion {
		t.Fatal("completed operation was restarted or unchanged status was rewritten")
	}
	got.Status = infra.GPUClusterStatus{}
	if err := c.Status().Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	provisioningReconcileOK(t, r, key)
	if restored := readProvisioningCluster(t, c, key); restored.Status.RunID != "completed-run" || !reflect.DeepEqual(restored.Status.Hosts, before.Status.Hosts) || ops.starts != 1 {
		t.Fatalf("status wipe did not recover original result: %+v; starts=%d", restored.Status, ops.starts)
	}
}

func TestProvisioningFailureAndProviderUncertainty(t *testing.T) {
	c, key, cluster := provisioningClient(t, true)
	provider := &provisioningInventory{SimulatedAllocator: &lab.SimulatedAllocator{}}
	for slot := 1; slot <= 2; slot++ {
		if _, err := provider.EnsureHost(t.Context(), string(cluster.UID), slot); err != nil {
			t.Fatal(err)
		}
	}
	hosts, _ := provider.ListHosts(t.Context(), string(cluster.UID))
	id := ProvisioningWorkflowID(string(cluster.UID), cluster.Generation)
	ops := &recordedOperations{states: map[string]OperationState{id: {Phase: PhaseReady, RunID: "ready-run", Hosts: hosts}}}
	r := &ProvisioningReconciler{Client: c, Provider: provider, Operations: ops}
	provisioningReconcileOK(t, r, key)
	before := readProvisioningCluster(t, c, key)
	provider.listErr = errors.New("provider unavailable")
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); !errors.Is(err, provider.listErr) {
		t.Fatalf("provider error = %v", err)
	}
	got := readProvisioningCluster(t, c, key)
	if !reflect.DeepEqual(got.Status.Hosts, before.Status.Hosts) || got.Status.AllocatedHosts != 2 || got.Status.ReadyHosts != 2 {
		t.Fatalf("uncertain observation erased last-known inventory: %+v", got.Status)
	}
	if condition := apimeta.FindStatusCondition(got.Status.Conditions, "Allocated"); condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "ProviderError" {
		t.Fatalf("uncertain inventory condition = %+v", condition)
	}
	provider.listErr = nil
	ops.states[id] = OperationState{Phase: PhaseFailed, RunID: "failed-run", LastError: "unsupported GPU class"}
	provisioningReconcileOK(t, r, key)
	got = readProvisioningCluster(t, c, key)
	if got.Status.Phase != PhaseFailed || !strings.Contains(got.Status.LastError, "unsupported GPU class") || got.Status.ReadyHosts != 0 {
		t.Fatalf("terminal status = %+v", got.Status)
	}
	if condition := apimeta.FindStatusCondition(got.Status.Conditions, "Ready"); condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ProvisioningFailed" {
		t.Fatalf("terminal Ready condition = %+v", condition)
	}
	provisioningReconcileOK(t, r, key)
	if ops.starts != 0 {
		t.Fatal("terminal operation restarted")
	}
}

func TestProvisioningDeletionFencesBeforeCancelAndWaits(t *testing.T) {
	c, key, cluster := provisioningClient(t, true)
	var events []string
	provider := &provisioningInventory{SimulatedAllocator: &lab.SimulatedAllocator{}, events: &events}
	if _, err := provider.EnsureHost(t.Context(), string(cluster.UID), 1); err != nil {
		t.Fatal(err)
	}
	id := ProvisioningWorkflowID(string(cluster.UID), cluster.Generation)
	ops := &recordedOperations{states: map[string]OperationState{id: {Phase: PhaseProvisioning, RunID: "running-run"}}, events: &events}
	r := &ProvisioningReconciler{Client: c, Provider: provider, Operations: ops}
	// Deliberately no status identity: metadata retains the original workflow.
	if err := c.Delete(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	provider.releaseErr = errors.New("cannot fence provider")
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); !errors.Is(err, provider.releaseErr) {
		t.Fatalf("release error = %v", err)
	}
	if !reflect.DeepEqual(events, []string{"release"}) {
		t.Fatalf("canceled before successful fence: %v", events)
	}
	events = nil
	provider.releaseErr = nil
	result := provisioningReconcileOK(t, r, key)
	if !reflect.DeepEqual(events, []string{"release", "observe", "cancel"}) || result.RequeueAfter <= 0 {
		t.Fatalf("deletion order = %v; result=%+v", events, result)
	}
	if !controllerutil.ContainsFinalizer(readProvisioningCluster(t, c, key), AllocationFinalizer) || ops.cancelIDs[0] != id {
		t.Fatal("active workflow lost finalizer or cancellation used missing status identity")
	}
	if _, err := provider.EnsureHost(t.Context(), string(cluster.UID), 2); !errors.Is(err, lab.ErrOwnerReleased) {
		t.Fatalf("late allocation was not fenced: %v", err)
	}
	hosts, _ := provider.ListHosts(t.Context(), string(cluster.UID))
	if len(hosts) != 0 {
		t.Fatalf("fenced deletion retained hosts: %v", hosts)
	}
	ops.states[id] = OperationState{Phase: PhaseFailed, RunID: "running-run", LastError: "canceled"}
	provisioningReconcileOK(t, r, key)
	if err := c.Get(t.Context(), key, &infra.GPUCluster{}); !apierrors.IsNotFound(err) {
		t.Fatalf("terminal workflow retained object: %v", err)
	}
	provisioningReconcileOK(t, r, key)
}

func TestProvisioningDeletionWithoutWorkflow(t *testing.T) {
	c, key, cluster := provisioningClient(t, true)
	ops := &recordedOperations{states: map[string]OperationState{}}
	r := &ProvisioningReconciler{Client: c, Provider: &provisioningInventory{SimulatedAllocator: &lab.SimulatedAllocator{}}, Operations: ops}
	if err := c.Delete(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	provisioningReconcileOK(t, r, key)
	if err := c.Get(t.Context(), key, &infra.GPUCluster{}); !apierrors.IsNotFound(err) || len(ops.requests) != 0 {
		t.Fatalf("never-started deletion = %v; starts=%d", err, len(ops.requests))
	}
}

func TestProvisioningKnownTerminalStatusDoesNotRestartMissingHistory(t *testing.T) {
	c, key, cluster := provisioningClient(t, true)
	cluster = readProvisioningCluster(t, c, key)
	cluster.Status = infra.GPUClusterStatus{
		WorkflowID: ProvisioningWorkflowID(string(cluster.UID), cluster.Generation),
		RunID:      "original-run", Phase: PhaseReady,
	}
	if err := c.Status().Update(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	ops := &recordedOperations{states: map[string]OperationState{}, onStart: func(context.Context, provisioning.Request) OperationState {
		return OperationState{Phase: PhaseProvisioning, RunID: "unexpected-new-run"}
	}}
	r := &ProvisioningReconciler{Client: c, Provider: &provisioningInventory{SimulatedAllocator: &lab.SimulatedAllocator{}}, Operations: ops}
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	if !errors.Is(err, ErrOperationNotFound) || ops.starts != 0 {
		t.Fatalf("known terminal identity restarted after history disappeared: starts=%d error=%v", ops.starts, err)
	}
	if got := readProvisioningCluster(t, c, key); got.Status.RunID != "original-run" || got.Status.Phase != PhaseReady {
		t.Fatalf("lost durable identity after missing history: %+v", got.Status)
	} else if ready := apimeta.FindStatusCondition(got.Status.Conditions, "Ready"); ready == nil || ready.Status != metav1.ConditionUnknown || ready.Reason != "TemporalError" {
		t.Fatalf("missing history did not surface uncertainty: %+v", ready)
	}
}

func TestProvisioningMigratesLegacyWorkflowIdentityBeforeWorkflowCalls(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "deleting"}[deleting], func(t *testing.T) {
			c, key, cluster := provisioningClient(t, true)
			originalID := cluster.Annotations[WorkflowIDAnnotation]
			cluster = readProvisioningCluster(t, c, key)
			delete(cluster.Annotations, WorkflowIDAnnotation)
			cluster.Generation++
			if err := c.Update(t.Context(), cluster); err != nil {
				t.Fatal(err)
			}
			cluster.Status = infra.GPUClusterStatus{WorkflowID: originalID, RunID: "legacy-run", Phase: PhaseProvisioning}
			if err := c.Status().Update(t.Context(), cluster); err != nil {
				t.Fatal(err)
			}
			if deleting {
				if err := c.Delete(t.Context(), cluster); err != nil {
					t.Fatal(err)
				}
			}
			var events []string
			ops := &recordedOperations{states: map[string]OperationState{originalID: {Phase: PhaseProvisioning, RunID: "legacy-run"}}, events: &events}
			r := &ProvisioningReconciler{Client: c, Provider: &provisioningInventory{SimulatedAllocator: &lab.SimulatedAllocator{}}, Operations: ops}
			provisioningReconcileOK(t, r, key)
			migrated := readProvisioningCluster(t, c, key)
			if migrated.Annotations[WorkflowIDAnnotation] != originalID || len(ops.requests) != 0 || len(events) != 0 {
				t.Fatalf("migration called workflow before recording original identity: annotation=%v requests=%v events=%v", migrated.Annotations, ops.requests, events)
			}
			provisioningReconcileOK(t, r, key)
			if deleting {
				if len(ops.cancelIDs) != 1 || ops.cancelIDs[0] != originalID || !controllerutil.ContainsFinalizer(readProvisioningCluster(t, c, key), AllocationFinalizer) {
					t.Fatalf("legacy deletion used wrong workflow: cancels=%v", ops.cancelIDs)
				}
			} else if len(ops.ids) != 1 || ops.ids[0] != originalID || ops.starts != 0 {
				t.Fatalf("legacy migration dispatched new revision: ids=%v starts=%d", ops.ids, ops.starts)
			}
		})
	}
}

func TestProvisioningDeletionWithoutKnownIdentityStaysFenced(t *testing.T) {
	for _, identity := range []string{"", "gpucluster/unrelated-owner/generation/3", "gpucluster/owner-123/generation/999"} {
		t.Run(identity, func(t *testing.T) {
			c, key, cluster := provisioningClient(t, true)
			cluster = readProvisioningCluster(t, c, key)
			cluster.Annotations[WorkflowIDAnnotation] = identity
			if err := c.Update(t.Context(), cluster); err != nil {
				t.Fatal(err)
			}
			provider := &provisioningInventory{SimulatedAllocator: &lab.SimulatedAllocator{}}
			if _, err := provider.EnsureHost(t.Context(), string(cluster.UID), 1); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), cluster); err != nil {
				t.Fatal(err)
			}
			ops := &recordedOperations{states: map[string]OperationState{}}
			r := &ProvisioningReconciler{Client: c, Provider: provider, Operations: ops}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err == nil {
				t.Fatal("unknown or invalid deletion identity treated as no workflow")
			}
			if !controllerutil.ContainsFinalizer(readProvisioningCluster(t, c, key), AllocationFinalizer) || len(ops.cancelIDs) != 0 {
				t.Fatal("uncertain workflow identity removed finalizer or targeted another owner")
			}
			if _, err := provider.EnsureHost(t.Context(), string(cluster.UID), 2); !errors.Is(err, lab.ErrOwnerReleased) {
				t.Fatalf("identity uncertainty did not fence provider: %v", err)
			}
		})
	}
}

func provisioningClient(t *testing.T, finalizer bool) (client.Client, client.ObjectKey, *infra.GPUCluster) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := infra.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &infra.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "lesson", UID: types.UID("owner-123"), Generation: 3}, Spec: infra.GPUClusterSpec{NodeCount: 2}}
	if finalizer {
		cluster.Finalizers = []string{AllocationFinalizer}
		cluster.Annotations = map[string]string{WorkflowIDAnnotation: ProvisioningWorkflowID(string(cluster.UID), cluster.Generation)}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&infra.GPUCluster{}).WithObjects(cluster).Build()
	return c, client.ObjectKeyFromObject(cluster), cluster
}

func readProvisioningCluster(t *testing.T, c client.Client, key client.ObjectKey) *infra.GPUCluster {
	t.Helper()
	cluster := &infra.GPUCluster{}
	if err := c.Get(t.Context(), key, cluster); err != nil {
		t.Fatal(err)
	}
	return cluster
}

func provisioningReconcileOK(t *testing.T, r *ProvisioningReconciler, key client.ObjectKey) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

type provisioningInventory struct {
	*lab.SimulatedAllocator
	listErr, releaseErr error
	events              *[]string
}

func (p *provisioningInventory) ListHosts(ctx context.Context, uid string) ([]lab.HostAllocation, error) {
	if p.listErr != nil {
		return nil, p.listErr
	}
	return p.SimulatedAllocator.ListHosts(ctx, uid)
}

func (p *provisioningInventory) ReleaseHosts(ctx context.Context, uid string) error {
	if p.events != nil {
		*p.events = append(*p.events, "release")
	}
	if p.releaseErr != nil {
		return p.releaseErr
	}
	return p.SimulatedAllocator.ReleaseHosts(ctx, uid)
}

// Models the remote Temporal boundary: once recorded, an identity is retained
// even after its workflow completes and Kubernetes loses every status field.
type recordedOperations struct {
	states    map[string]OperationState
	onStart   func(context.Context, provisioning.Request) OperationState
	starts    int
	requests  []provisioning.Request
	ids       []string
	cancelIDs []string
	events    *[]string
}

func (o *recordedOperations) Ensure(ctx context.Context, id string, req provisioning.Request) (OperationState, error) {
	o.ids = append(o.ids, id)
	o.requests = append(o.requests, req)
	if state, ok := o.states[id]; ok {
		return state, nil
	}
	o.starts++
	state := o.onStart(ctx, req)
	o.states[id] = state
	return state, nil
}

func (o *recordedOperations) Observe(_ context.Context, id string) (OperationState, error) {
	if o.events != nil {
		*o.events = append(*o.events, "observe")
	}
	state, ok := o.states[id]
	if !ok {
		return OperationState{}, ErrOperationNotFound
	}
	return state, nil
}

func (o *recordedOperations) Cancel(_ context.Context, id string) error {
	if o.events != nil {
		*o.events = append(*o.events, "cancel")
	}
	o.cancelIDs = append(o.cancelIDs, id)
	return nil
}

type provisioningStatusFault struct {
	client.Client
	err error
}

type provisioningPatchFault struct {
	client.Client
	err error
}

func (c *provisioningPatchFault) Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
	return c.err
}

func (c *provisioningStatusFault) Status() client.SubResourceWriter {
	return &provisioningStatusWriter{SubResourceWriter: c.Client.Status(), err: c.err}
}

type provisioningStatusWriter struct {
	client.SubResourceWriter
	err error
}

func (s *provisioningStatusWriter) Patch(context.Context, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
	return s.err
}
