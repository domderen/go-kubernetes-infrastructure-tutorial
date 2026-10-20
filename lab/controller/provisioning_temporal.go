package controller

import (
	"context"
	"errors"
	"fmt"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/provisioning"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	temporalclient "go.temporal.io/sdk/client"
)

// TemporalOperations retains operation identity in Temporal, independently of
// the reconciler's lifetime and the last status Kubernetes accepted.
type TemporalOperations struct {
	client    temporalclient.Client
	taskQueue string
}

func NewTemporalOperations(c temporalclient.Client, taskQueue string) *TemporalOperations {
	return &TemporalOperations{client: c, taskQueue: taskQueue}
}

var _ ProvisioningOperations = (*TemporalOperations)(nil)

func (o *TemporalOperations) Ensure(ctx context.Context, id string, req provisioning.Request) (OperationState, error) {
	state, err := o.Observe(ctx, id)
	if err == nil || !errors.Is(err, ErrOperationNotFound) {
		return state, err
	}
	_, err = o.client.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID: id, TaskQueue: o.taskQueue,
		// Running conflicts and closed identities are both rejected. In
		// particular, the SDK default AllowDuplicate would rerun a successful
		// workflow whenever its Kubernetes status had been lost.
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, provisioning.ProvisionCluster, req)
	var duplicate *serviceerror.WorkflowExecutionAlreadyStarted
	if err != nil && !errors.As(err, &duplicate) {
		// A lost start response is safe: the next Ensure observes the same ID.
		return OperationState{}, fmt.Errorf("start provisioning workflow: %w", err)
	}
	// A concurrent reconciler may have won the start, and a fast workflow may
	// already be closed. Report its real state instead of guessing Running.
	return o.Observe(ctx, id)
}

func (o *TemporalOperations) Observe(ctx context.Context, id string) (OperationState, error) {
	desc, err := o.client.DescribeWorkflowExecution(ctx, id, "")
	if err != nil {
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			return OperationState{}, ErrOperationNotFound
		}
		return OperationState{}, fmt.Errorf("describe provisioning workflow: %w", err)
	}
	info := desc.GetWorkflowExecutionInfo()
	if info == nil || info.GetExecution().GetRunId() == "" {
		return OperationState{}, fmt.Errorf("workflow %q has no execution identity", id)
	}
	state := OperationState{RunID: info.GetExecution().GetRunId()}
	switch info.GetStatus() {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		var hosts []lab.HostAllocation
		// Pin the observed run; never read a different execution's result.
		if err := o.client.GetWorkflow(ctx, id, state.RunID).Get(ctx, &hosts); err != nil {
			return OperationState{}, fmt.Errorf("read completed provisioning result: %w", err)
		}
		state.Phase, state.Hosts = PhaseReady, hosts
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		state.Phase = PhaseFailed
		state.LastError = info.GetStatus().String()
		if err := o.client.GetWorkflow(ctx, id, state.RunID).Get(ctx, nil); err != nil {
			state.LastError = err.Error()
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED:
		state.Phase = PhasePending
	default:
		state.Phase = PhaseProvisioning
	}
	return state, nil
}

func (o *TemporalOperations) Cancel(ctx context.Context, id string) error {
	if err := o.client.CancelWorkflow(ctx, id, ""); err != nil {
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			return ErrOperationNotFound
		}
		return err
	}
	return nil
}
