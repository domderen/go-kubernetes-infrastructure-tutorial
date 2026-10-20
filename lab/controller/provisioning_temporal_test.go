package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/provisioning"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	temporalclient "go.temporal.io/sdk/client"
)

func TestTemporalOperationsRecoversClosedIdentity(t *testing.T) {
	for _, status := range []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	} {
		t.Run(status.String(), func(t *testing.T) {
			hosts := []lab.HostAllocation{{ID: "retained-host", OwnerUID: "uid", Slot: 1}}
			c := &operationTemporalClient{exists: true, status: status, hosts: hosts}
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				c.resultErr = errors.New("terminal GPU validation failure")
			}
			op := NewTemporalOperations(c, "test-queue")
			state, err := op.Ensure(t.Context(), "fixed-workflow", provisioning.Request{ClusterUID: "uid"})
			if err != nil {
				t.Fatal(err)
			}
			if c.starts != 0 || state.RunID != "fixed-run" || c.getRunID != "fixed-run" || !state.terminal() {
				t.Fatalf("recovery restarted closed identity or read unpinned result: %+v; calls=%+v", state, c)
			}
			if status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				if state.Phase != PhaseReady || !reflect.DeepEqual(state.Hosts, hosts) {
					t.Fatalf("completed result = %+v", state)
				}
			} else if state.Phase != PhaseFailed || !strings.Contains(state.LastError, "terminal GPU validation failure") {
				t.Fatalf("terminal result = %+v", state)
			}
		})
	}
}

func TestTemporalOperationsStartRejectsDuplicateAndRecoversRace(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "concurrent start"}[race], func(t *testing.T) {
			c := &operationTemporalClient{status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}
			if race {
				c.startErr = serviceerror.NewWorkflowExecutionAlreadyStarted("other controller started", "request", "fixed-run")
			}
			req := provisioning.Request{ClusterUID: "uid", OperationID: "fixed-workflow", NodeCount: 2, ImageVersion: "os-v1", GPUClass: "mock-a100", MaxParallel: 2}
			state, err := NewTemporalOperations(c, "test-queue").Ensure(t.Context(), "fixed-workflow", req)
			if err != nil {
				t.Fatal(err)
			}
			if state.Phase != PhaseProvisioning || state.RunID != "fixed-run" || c.starts != 1 || !reflect.DeepEqual(c.request, req) {
				t.Fatalf("start result = %+v; starts=%d request=%+v", state, c.starts, c.request)
			}
			if c.options.ID != "fixed-workflow" || c.options.TaskQueue != "test-queue" || c.options.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE || c.options.WorkflowIDConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL || !c.options.WorkflowExecutionErrorWhenAlreadyStarted {
				t.Fatalf("unsafe workflow identity policy: %+v", c.options)
			}
			if c.options.RetryPolicy != nil {
				t.Fatal("workflow-level retries must not hot restart terminal failures")
			}
		})
	}
}

func TestTemporalOperationsDoesNotStartDuringUncertainObservation(t *testing.T) {
	outage := errors.New("Temporal connection unavailable")
	c := &operationTemporalClient{describeErr: outage}
	_, err := NewTemporalOperations(c, "queue").Ensure(t.Context(), "workflow", provisioning.Request{})
	if !errors.Is(err, outage) || c.starts != 0 {
		t.Fatalf("uncertain observation started operation: starts=%d error=%v", c.starts, err)
	}
	c.describeErr = nil
	op := NewTemporalOperations(c, "queue")
	if _, err := op.Observe(t.Context(), "absent"); !errors.Is(err, ErrOperationNotFound) {
		t.Fatalf("missing operation = %v", err)
	}
	c.cancelErr = serviceerror.NewNotFound("already gone")
	if err := op.Cancel(t.Context(), "absent"); err != nil && !errors.Is(err, ErrOperationNotFound) {
		t.Fatalf("cancel absent workflow = %v", err)
	}
}

func TestTemporalOperationsLostStartResponseRecoversOriginalRun(t *testing.T) {
	lost := errors.New("start response lost after server commit")
	c := &operationTemporalClient{status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, startErr: lost}
	op := NewTemporalOperations(c, "queue")
	if _, err := op.Ensure(t.Context(), "fixed-workflow", provisioning.Request{}); !errors.Is(err, lost) {
		t.Fatalf("lost start response = %v", err)
	}
	state, err := op.Ensure(t.Context(), "fixed-workflow", provisioning.Request{})
	if err != nil || state.RunID != "fixed-run" || state.Phase != PhaseProvisioning || c.starts != 1 {
		t.Fatalf("lost response created another run: state=%+v error=%v starts=%d", state, err, c.starts)
	}
}

type operationTemporalClient struct {
	temporalclient.Client
	exists      bool
	status      enumspb.WorkflowExecutionStatus
	hosts       []lab.HostAllocation
	describeErr error
	startErr    error
	resultErr   error
	cancelErr   error
	starts      int
	options     temporalclient.StartWorkflowOptions
	request     provisioning.Request
	getRunID    string
}

func (c *operationTemporalClient) DescribeWorkflowExecution(context.Context, string, string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if c.describeErr != nil {
		return nil, c.describeErr
	}
	if !c.exists {
		return nil, serviceerror.NewNotFound("not started")
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "fixed-workflow", RunId: "fixed-run"}, Status: c.status,
	}}, nil
}

func (c *operationTemporalClient) ExecuteWorkflow(_ context.Context, opts temporalclient.StartWorkflowOptions, _ interface{}, args ...interface{}) (temporalclient.WorkflowRun, error) {
	c.starts++
	c.options = opts
	c.request = args[0].(provisioning.Request)
	c.exists = true
	return &operationTemporalRun{hosts: c.hosts, err: c.resultErr}, c.startErr
}

func (c *operationTemporalClient) GetWorkflow(_ context.Context, _ string, runID string) temporalclient.WorkflowRun {
	c.getRunID = runID
	return &operationTemporalRun{hosts: c.hosts, err: c.resultErr}
}

func (c *operationTemporalClient) CancelWorkflow(context.Context, string, string) error {
	return c.cancelErr
}

type operationTemporalRun struct {
	temporalclient.WorkflowRun
	hosts []lab.HostAllocation
	err   error
}

func (r *operationTemporalRun) Get(_ context.Context, value interface{}) error {
	if r.err != nil {
		return r.err
	}
	if value != nil {
		*value.(*[]lab.HostAllocation) = append([]lab.HostAllocation(nil), r.hosts...)
	}
	return nil
}
