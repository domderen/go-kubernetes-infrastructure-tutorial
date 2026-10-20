package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	infra "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/api/v1alpha1"
	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/provisioning"
	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// WorkflowIDAnnotation pins the original provisioning revision before
	// dispatch. Kubernetes may change generation when deletion begins.
	WorkflowIDAnnotation = "infrastructure.tutorial.example.com/workflow-id"

	PhasePending      = "Pending"
	PhaseProvisioning = "Provisioning"
	PhaseReady        = "Ready"
	PhaseFailed       = "Failed"
)

var ErrOperationNotFound = errors.New("provisioning operation not found")

// LifecycleInventory deliberately cannot allocate. Only workflow activities
// create hosts; the controller observes them and fences an owner on deletion.
type LifecycleInventory interface {
	ListHosts(context.Context, string) ([]lab.HostAllocation, error)
	ReleaseHosts(context.Context, string) error
}

// OperationState is an observation of a durable Temporal execution. Ready and
// Failed are terminal, including cancellation/termination in Failed.
type OperationState struct {
	Phase     string
	RunID     string
	Hosts     []lab.HostAllocation
	LastError string
}

func (s OperationState) terminal() bool { return s.Phase == PhaseReady || s.Phase == PhaseFailed }

// ProvisioningOperations must retain completed identities: Ensure may observe
// an existing execution but must never restart it after a lost status write.
type ProvisioningOperations interface {
	Ensure(context.Context, string, provisioning.Request) (OperationState, error)
	Observe(context.Context, string) (OperationState, error)
	Cancel(context.Context, string) error
}

// ProvisioningWorkflowID derives the initial intent. Persist it in metadata
// before dispatch; later metadata generations are not new provisioning intents.
func ProvisioningWorkflowID(uid string, generation int64) string {
	return fmt.Sprintf("gpucluster/%s/generation/%d", uid, generation)
}

type ProvisioningReconciler struct {
	client.Client
	Provider   LifecycleInventory
	Operations ProvisioningOperations
	// PollInterval refreshes operation and provider observations; default 2s.
	PollInterval time.Duration
}

func (r *ProvisioningReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Status/finalizer updates must also trigger recovery; generation alone
	// would miss a lost status report and the finalizer-first second pass.
	return ctrl.NewControllerManagedBy(mgr).Named("gpucluster-provisioning").For(&infra.GPUCluster{}).Complete(r)
}

func (r *ProvisioningReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cluster := &infra.GPUCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		return r.reconcileDeletion(ctx, cluster)
	}
	id, err := recordedProvisioningID(cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if id == "" {
		id = ProvisioningWorkflowID(string(cluster.UID), cluster.Generation)
	}
	if !controllerutil.ContainsFinalizer(cluster, AllocationFinalizer) || cluster.Annotations[WorkflowIDAnnotation] == "" {
		before := cluster.DeepCopy()
		controllerutil.AddFinalizer(cluster, AllocationFinalizer)
		setWorkflowIDAnnotation(cluster, id)
		if err := r.Patch(ctx, cluster, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, fmt.Errorf("persist provisioning identity and finalizer: %w", err)
		}
		return ctrl.Result{}, nil
	}

	before := cluster.DeepCopy()
	var state OperationState
	if cluster.Status.WorkflowID == id && (OperationState{Phase: cluster.Status.Phase}).terminal() {
		// A known terminal report is an additional durable stop marker. If
		// Temporal retention later removes its history, missing history is
		// uncertainty rather than permission to dispatch the completed intent.
		state, err = r.Operations.Observe(ctx, id)
	} else {
		state, err = r.Operations.Ensure(ctx, id, provisioningRequest(cluster, id))
	}
	cluster.Status.WorkflowID = id
	if err != nil {
		if cluster.Status.Phase == "" {
			cluster.Status.Phase = PhasePending
		}
		cluster.Status.LastError = err.Error()
		setProvisioningCondition(cluster, "Ready", metav1.ConditionUnknown, "TemporalError", err.Error())
		return ctrl.Result{}, errors.Join(err, r.patchProvisioningStatus(ctx, before, cluster))
	}
	cluster.Status.WorkflowID = id
	cluster.Status.RunID = state.RunID
	cluster.Status.Phase = state.Phase
	cluster.Status.LastError = state.LastError
	cluster.Status.ObservedGeneration = cluster.Generation
	hosts, err := r.Provider.ListHosts(ctx, string(cluster.UID))
	if err != nil {
		// Neither a failed lookup nor a failed RPC proves capacity vanished.
		// Retain the complete last-known inventory and ready count.
		cluster.Status.LastError = err.Error()
		if state.LastError != "" {
			cluster.Status.LastError = state.LastError + "; " + err.Error()
		}
		setProvisioningCondition(cluster, "Allocated", metav1.ConditionUnknown, "ProviderError", err.Error())
		if state.Phase == PhaseFailed {
			setProvisioningCondition(cluster, "Ready", metav1.ConditionFalse, "ProvisioningFailed", state.LastError)
		} else {
			setProvisioningCondition(cluster, "Ready", metav1.ConditionUnknown, "ProviderError", err.Error())
		}
		return ctrl.Result{}, errors.Join(err, r.patchProvisioningStatus(ctx, before, cluster))
	}
	r.projectInventory(cluster, state, hosts)
	if err := r.patchProvisioningStatus(ctx, before, cluster); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.pollInterval()}, nil
}

func (r *ProvisioningReconciler) reconcileDeletion(ctx context.Context, cluster *infra.GPUCluster) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(cluster, AllocationFinalizer) {
		return ctrl.Result{}, nil
	}
	// Closing the provider UID comes first, even when Temporal is unavailable.
	// It fences activities already running or still queued on a stopped worker.
	if err := r.Provider.ReleaseHosts(ctx, string(cluster.UID)); err != nil {
		return ctrl.Result{}, fmt.Errorf("fence and release hosts: %w", err)
	}
	id, err := recordedProvisioningID(cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if id == "" {
		// Legacy objects may have dispatched work before metadata identity was
		// introduced. An absent status cannot prove that no workflow exists,
		// and the deletion generation is not the provisioning revision.
		return ctrl.Result{}, fmt.Errorf("cannot confirm workflow termination: provisioning identity is missing; provider owner remains fenced")
	}
	if cluster.Annotations[WorkflowIDAnnotation] == "" {
		before := cluster.DeepCopy()
		setWorkflowIDAnnotation(cluster, id)
		if err := r.Patch(ctx, cluster, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, fmt.Errorf("persist legacy deleting workflow identity: %w", err)
		}
		return ctrl.Result{RequeueAfter: r.pollInterval()}, nil
	}
	state, err := r.Operations.Observe(ctx, id)
	if err != nil && !errors.Is(err, ErrOperationNotFound) {
		return ctrl.Result{}, fmt.Errorf("observe deleting workflow: %w", err)
	}
	if err == nil && !state.terminal() {
		if err := r.Operations.Cancel(ctx, id); err != nil && !errors.Is(err, ErrOperationNotFound) {
			return ctrl.Result{}, fmt.Errorf("cancel deleting workflow: %w", err)
		}
		// Cancellation is a request, not proof that activities and compensation
		// have finished. Keep protection until a later terminal observation.
		return ctrl.Result{RequeueAfter: r.pollInterval()}, nil
	}
	before := cluster.DeepCopy()
	controllerutil.RemoveFinalizer(cluster, AllocationFinalizer)
	return ctrl.Result{}, r.Patch(ctx, cluster, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

// Metadata is authoritative. Valid legacy status is migrated to metadata
// before any workflow call; unrelated owners or impossible revisions block.
func recordedProvisioningID(cluster *infra.GPUCluster) (string, error) {
	id := cluster.Annotations[WorkflowIDAnnotation]
	if id == "" {
		id = cluster.Status.WorkflowID
	}
	if id == "" {
		return "", nil
	}
	prefix := fmt.Sprintf("gpucluster/%s/generation/", cluster.UID)
	if !strings.HasPrefix(id, prefix) {
		return "", fmt.Errorf("recorded workflow identity %q does not belong to GPUCluster UID %q", id, cluster.UID)
	}
	revision := strings.TrimPrefix(id, prefix)
	generation, err := strconv.ParseInt(revision, 10, 64)
	if err != nil || generation <= 0 || generation > cluster.Generation || strconv.FormatInt(generation, 10) != revision {
		return "", fmt.Errorf("recorded workflow identity %q has an invalid provisioning generation", id)
	}
	return id, nil
}

func setWorkflowIDAnnotation(cluster *infra.GPUCluster, id string) {
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations[WorkflowIDAnnotation] = id
}

func provisioningRequest(cluster *infra.GPUCluster, id string) provisioning.Request {
	req := provisioning.Request{
		ClusterUID: string(cluster.UID), NodeCount: int(cluster.Spec.NodeCount), OperationID: id,
		ImageVersion: cluster.Spec.ImageVersion, GPUClass: cluster.Spec.GPUClass, MaxParallel: int(cluster.Spec.MaxParallel),
	}
	// Kubernetes admission defaults these; explicit defaults also support fake
	// clients and resources created before the additive schema fields existed.
	if req.ImageVersion == "" {
		req.ImageVersion = "os-v1"
	}
	if req.GPUClass == "" {
		req.GPUClass = "mock-a100"
	}
	if req.MaxParallel == 0 {
		req.MaxParallel = 2
	}
	return req
}

func (r *ProvisioningReconciler) projectInventory(cluster *infra.GPUCluster, state OperationState, hosts []lab.HostAllocation) {
	// Stable status ordering prevents incidental map iteration from writing on
	// every poll. Provider inventory remains authoritative for allocated hosts.
	hosts = append([]lab.HostAllocation(nil), hosts...)
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].Slot < hosts[j].Slot })
	cluster.Status.AllocatedHosts = int32(len(hosts))
	cluster.Status.Hosts = nil
	present := make(map[infra.AllocatedHost]bool, len(hosts))
	for _, host := range hosts {
		observed := infra.AllocatedHost{ID: host.ID, Slot: int32(host.Slot)}
		cluster.Status.Hosts = append(cluster.Status.Hosts, observed)
		present[observed] = true
	}
	if int32(len(hosts)) == cluster.Spec.NodeCount {
		setProvisioningCondition(cluster, "Allocated", metav1.ConditionTrue, "HostsAllocated", "All requested simulated host allocations are present.")
	} else {
		setProvisioningCondition(cluster, "Allocated", metav1.ConditionFalse, "AllocationsIncomplete", "Observed allocation count differs from requested capacity.")
	}
	cluster.Status.ReadyHosts = 0
	switch state.Phase {
	case PhaseReady:
		for _, host := range state.Hosts {
			if present[infra.AllocatedHost{ID: host.ID, Slot: int32(host.Slot)}] {
				cluster.Status.ReadyHosts++
			}
		}
		if cluster.Status.ReadyHosts == cluster.Spec.NodeCount && cluster.Status.AllocatedHosts == cluster.Spec.NodeCount {
			setProvisioningCondition(cluster, "Ready", metav1.ConditionTrue, "ProvisioningComplete", "The simulated image, membership, and GPU validation lifecycle completed.")
		} else {
			cluster.Status.LastError = "completed workflow result does not match current provider inventory"
			setProvisioningCondition(cluster, "Ready", metav1.ConditionFalse, "InventoryMismatch", cluster.Status.LastError)
		}
	case PhaseFailed:
		setProvisioningCondition(cluster, "Ready", metav1.ConditionFalse, "ProvisioningFailed", state.LastError)
	default:
		setProvisioningCondition(cluster, "Ready", metav1.ConditionFalse, "Provisioning", "The durable simulated provisioning operation is in progress.")
	}
}

func setProvisioningCondition(cluster *infra.GPUCluster, kind string, status metav1.ConditionStatus, reason, message string) {
	apimeta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: kind, Status: status, Reason: reason, Message: message, ObservedGeneration: cluster.Generation,
	})
}

func (r *ProvisioningReconciler) pollInterval() time.Duration {
	if r.PollInterval > 0 {
		return r.PollInterval
	}
	return 2 * time.Second
}

func (r *ProvisioningReconciler) patchProvisioningStatus(ctx context.Context, before, cluster *infra.GPUCluster) error {
	if equality.Semantic.DeepEqual(before.Status, cluster.Status) {
		return nil
	}
	return r.Status().Patch(ctx, cluster, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}
