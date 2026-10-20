package provisioning

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

// Catches returning imaged allocations before every host has joined and passed
// GPU validation, or replacing the stable UID/intent/slot identity.
func TestProvisionClusterCompletesHostLifecycle(t *testing.T) {
	provider := newLifecycleProvider()
	env := lifecycleEnvironment(provider)
	env.ExecuteWorkflow(ProvisionCluster, Request{ClusterUID: "cluster-A", NodeCount: 2, OperationID: "revision-1", ImageVersion: "os-v1"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var hosts []lab.HostAllocation
	if err := env.GetWorkflowResult(&hosts); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	want := []lab.HostOperation{
		{OwnerUID: "cluster-A", OperationID: "revision-1/join/1", HostID: "host-001", GPUClass: "mock-a100"},
		{OwnerUID: "cluster-A", OperationID: "revision-1/validate/1", HostID: "host-001", GPUClass: "mock-a100"},
		{OwnerUID: "cluster-A", OperationID: "revision-1/join/2", HostID: "host-002", GPUClass: "mock-a100"},
		{OwnerUID: "cluster-A", OperationID: "revision-1/validate/2", HostID: "host-002", GPUClass: "mock-a100"},
	}
	if len(hosts) != 2 || !reflect.DeepEqual(provider.operations, want) {
		t.Fatalf("hosts=%v lifecycle=%+v; want two validated hosts and %+v", hosts, provider.operations, want)
	}
}

// Catches sequential execution despite an explicit bound, or unbounded fan-out.
func TestProvisionClusterBoundsParallelHostLifecycles(t *testing.T) {
	provider := newLifecycleProvider()
	provider.imageDelay = 20 * time.Millisecond
	env := lifecycleEnvironment(provider)
	env.ExecuteWorkflow(ProvisionCluster, Request{ClusterUID: "cluster-A", NodeCount: 6, OperationID: "revision-1", ImageVersion: "os-v1", GPUClass: "mock-h100", MaxParallel: 2})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.maxPipelines != 2 || len(provider.operations) != 12 || len(provider.activeHosts) != 0 {
		t.Fatalf("max host pipelines=%d lifecycle steps=%d pending hosts=%v; want 2, 12, and no pending hosts", provider.maxPipelines, len(provider.operations), provider.activeHosts)
	}
	for _, operation := range provider.operations {
		if operation.GPUClass != "mock-h100" {
			t.Fatalf("GPU class not carried through lifecycle: %+v", operation)
		}
	}
}

func TestProvisionClusterRejectsInvalidParallelismBeforeReservation(t *testing.T) {
	for _, limit := range []int{-1, 11} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			provider := newLifecycleProvider()
			env := lifecycleEnvironment(provider)
			env.ExecuteWorkflow(ProvisionCluster, Request{ClusterUID: "cluster-A", NodeCount: 2, OperationID: "revision-1", ImageVersion: "os-v1", MaxParallel: limit})
			assertNonRetryable(t, env.GetWorkflowError())
			keys, _, lists := provider.snapshot()
			if lists != 0 || len(keys) != 0 {
				t.Fatalf("invalid limit touched provider: lists=%d keys=%v", lists, keys)
			}
		})
	}
}

// Catches changing a later-step key on retry or re-running earlier successful
// steps when a join response is lost after its provider effect committed.
func TestProvisionClusterRetriesJoinWithStableIdentity(t *testing.T) {
	provider := newLifecycleProvider()
	provider.loseJoinResponse = true
	env := lifecycleEnvironment(provider)
	env.ExecuteWorkflow(ProvisionCluster, Request{ClusterUID: "cluster-A", NodeCount: 1, OperationID: "revision-1", ImageVersion: "os-v1"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.operations) != 3 || provider.operations[0] != provider.operations[1] || provider.operations[2].OperationID != "revision-1/validate/1" {
		t.Fatalf("retry identity=%+v; want unchanged join request retried, then validation", provider.operations)
	}
	requests, _ := provider.imageSnapshot()
	if len(requests) != 1 {
		t.Fatalf("completed imaging repeated after join retry: %+v", requests)
	}
}

// Catches retrying terminal validation, continuing to later hosts, or leaving
// reserved capacity behind when the simulated lifecycle cannot finish.
func TestProvisionClusterCompensatesTerminalValidationFailure(t *testing.T) {
	for _, failure := range []error{lab.ErrValidationFailed, lab.ErrUnsupportedGPUClass, lab.ErrOwnerReleased} {
		t.Run(failure.Error(), func(t *testing.T) {
			provider := newLifecycleProvider()
			provider.validationErr = failure
			env := lifecycleEnvironment(provider)
			env.ExecuteWorkflow(ProvisionCluster, Request{ClusterUID: "cluster-A", NodeCount: 3, OperationID: "revision-1", ImageVersion: "os-v1"})
			assertNonRetryable(t, env.GetWorkflowError())
			provider.mu.Lock()
			calls, releases := len(provider.operations), provider.releaseCalls
			provider.mu.Unlock()
			hosts, err := provider.ListHosts(t.Context(), "cluster-A")
			if calls != 2 || releases != 1 || err != nil || len(hosts) != 0 {
				t.Fatalf("steps=%d releases=%d hosts=%v error=%v; want one failed validation and released reservations", calls, releases, hosts, err)
			}
		})
	}
}

func TestProvisionClusterPreservesFailureWhenCleanupFails(t *testing.T) {
	provider := newLifecycleProvider()
	provider.validationErr = lab.ErrValidationFailed
	provider.cleanupErr = errors.New("cleanup provider unavailable")
	env := lifecycleEnvironment(provider)
	env.ExecuteWorkflow(ProvisionCluster, Request{ClusterUID: "cluster-A", NodeCount: 1, OperationID: "revision-1", ImageVersion: "os-v1"})
	err := env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), lab.ErrValidationFailed.Error()) || !strings.Contains(err.Error(), "cleanup provider unavailable") {
		t.Fatalf("workflow error=%v; want original validation and cleanup failures", err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.releaseCalls != 3 {
		t.Fatalf("cleanup attempts=%d; want bounded total of 3", provider.releaseCalls)
	}
}

// Catches returning a sibling's cancellation instead of the first failure,
// failing to cancel outstanding work, or dispatching later hosts after failure.
func TestProvisionClusterFailureCancelsOutstandingHost(t *testing.T) {
	provider := newLifecycleProvider()
	provider.validationErr = lab.ErrValidationFailed
	provider.blockHost = "host-002"
	provider.imageStarted = make(chan struct{}, 1)
	provider.imageStopped = make(chan struct{}, 1)
	provider.waitForBlockedImage = true
	env := lifecycleEnvironment(provider)
	activityCtx, stopActivities := context.WithCancelCause(t.Context())
	defer stopActivities(nil)
	env.SetWorkerOptions(worker.Options{BackgroundActivityContext: activityCtx})
	env.ExecuteWorkflow(ProvisionCluster, Request{ClusterUID: "cluster-A", NodeCount: 5, OperationID: "revision-1", ImageVersion: "os-v1", MaxParallel: 2})
	stopActivities(temporal.NewCanceledError())
	select {
	case <-provider.imageStopped:
	case <-time.After(time.Second):
		t.Fatal("blocked sibling activity did not stop")
	}
	err := env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), lab.ErrValidationFailed.Error()) || temporal.IsCanceledError(err) {
		t.Fatalf("workflow error=%v; want the original validation failure", err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	requests, _ := provider.imageSnapshot()
	if provider.releaseCalls != 1 || len(requests) != 2 || len(provider.operations) != 2 || provider.canceledImages != 1 {
		t.Fatalf("releases=%d images=%d steps=%v canceled=%d; want first failure to stop and compensate", provider.releaseCalls, len(requests), provider.operations, provider.canceledImages)
	}
}

func TestProvisionClusterCompensatesPartialReservationFailure(t *testing.T) {
	provider := newLifecycleProvider()
	provider.failAllocationSlot = 2
	env := lifecycleEnvironment(provider)
	env.ExecuteWorkflow(ProvisionCluster, Request{ClusterUID: "cluster-A", NodeCount: 3, OperationID: "revision-1", ImageVersion: "os-v1"})
	if err := env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), "slot capacity unavailable") {
		t.Fatalf("workflow error=%v; want reservation failure", err)
	}
	hosts, err := provider.ListHosts(t.Context(), "cluster-A")
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if err != nil || len(hosts) != 0 || provider.releaseCalls != 1 {
		t.Fatalf("hosts=%v error=%v releases=%d; want partial reservation compensated", hosts, err, provider.releaseCalls)
	}
}

// Catches skipping cleanup on workflow cancellation or dispatching queued hosts
// afterwards. The SDK suite immediately acknowledges activity cancellation;
// actual server acknowledgment ordering requires the live integration test.
func TestProvisionClusterCancellationStopsDispatchAndCompensates(t *testing.T) {
	provider := newLifecycleProvider()
	provider.blockImages = true
	provider.imageStarted = make(chan struct{}, 2)
	provider.imageStopped = make(chan struct{}, 2)
	env := lifecycleEnvironment(provider)
	activityCtx, stopActivities := context.WithCancelCause(t.Context())
	defer stopActivities(nil)
	env.SetWorkerOptions(worker.Options{BackgroundActivityContext: activityCtx})
	cancelDone := make(chan struct{})
	go func() {
		defer close(cancelDone)
		for range 2 {
			select {
			case <-provider.imageStarted:
			case <-time.After(2 * time.Second):
				return
			}
		}
		env.CancelWorkflow()
	}()
	env.ExecuteWorkflow(ProvisionCluster, Request{ClusterUID: "cluster-A", NodeCount: 5, OperationID: "revision-1", ImageVersion: "os-v1", MaxParallel: 2})
	<-cancelDone
	// The SDK test environment resolves canceled futures before stopping the
	// in-process activities. Stop and join those fixtures explicitly.
	stopActivities(temporal.NewCanceledError())
	for range 2 {
		select {
		case <-provider.imageStopped:
		case <-time.After(time.Second):
			t.Fatal("activity fixture did not stop")
		}
	}
	if !temporal.IsCanceledError(env.GetWorkflowError()) {
		t.Fatalf("workflow error=%v; want cancellation", env.GetWorkflowError())
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	requests, jobs := provider.imageSnapshot()
	if provider.releaseCalls != 1 || provider.canceledImages != 2 || len(requests) != 2 || len(jobs) != 2 || len(provider.operations) != 0 {
		t.Fatalf("releases=%d canceled=%d images=%d jobs=%d steps=%v; want two started jobs, no later work, and cleanup", provider.releaseCalls, provider.canceledImages, len(requests), len(jobs), provider.operations)
	}
}

func lifecycleEnvironment(provider *lifecycleProvider) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(3 * time.Second)
	env.RegisterWorkflow(ProvisionCluster)
	allocation := &Activities{Provider: provider}
	imaging := &ImagingActivities{Provider: provider, PollInterval: time.Millisecond}
	env.RegisterActivityWithOptions(allocation.ReserveHosts, activity.RegisterOptions{Name: ReserveHostsActivity})
	env.RegisterActivityWithOptions(imaging.EnsureImage, activity.RegisterOptions{Name: EnsureImageActivity})
	lifecycle := &LifecycleActivities{Provider: provider}
	env.RegisterActivityWithOptions(lifecycle.JoinCluster, activity.RegisterOptions{Name: JoinClusterActivity})
	env.RegisterActivityWithOptions(lifecycle.ValidateGPU, activity.RegisterOptions{Name: ValidateGPUActivity})
	env.RegisterActivityWithOptions(lifecycle.ReleaseHosts, activity.RegisterOptions{Name: ReleaseHostsActivity})
	return env
}

// The existing allocator owns real allocations and image records. These extra
// simulated provider actions enforce sequencing at the side-effect boundary.
type lifecycleProvider struct {
	*faultProvider
	mu                  sync.Mutex
	operations          []lab.HostOperation
	joined              map[string]bool
	imageDelay          time.Duration
	activeHosts         map[string]bool
	maxPipelines        int
	loseJoinResponse    bool
	validationErr       error
	cleanupErr          error
	releaseCalls        int
	blockImages         bool
	blockHost           string
	imageStarted        chan struct{}
	imageStopped        chan struct{}
	canceledImages      int
	waitForBlockedImage bool
	failAllocationSlot  int
}

func newLifecycleProvider() *lifecycleProvider {
	return &lifecycleProvider{faultProvider: &faultProvider{SimulatedAllocator: &lab.SimulatedAllocator{}}, joined: make(map[string]bool)}
}

func (p *lifecycleProvider) EnsureImageJob(ctx context.Context, req lab.ImageRequest) (lab.ImageJob, error) {
	p.mu.Lock()
	if p.activeHosts == nil {
		p.activeHosts = make(map[string]bool)
	}
	p.activeHosts[req.HostID] = true
	if len(p.activeHosts) > p.maxPipelines {
		p.maxPipelines = len(p.activeHosts)
	}
	delay := p.imageDelay
	p.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return lab.ImageJob{}, ctx.Err()
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return lab.ImageJob{}, err
	}
	return p.faultProvider.EnsureImageJob(ctx, req)
}

func (p *lifecycleProvider) JoinCluster(ctx context.Context, req lab.HostOperation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	requests, jobs := p.imageSnapshot()
	complete := false
	for i, imageReq := range requests {
		if imageReq.HostID == req.HostID {
			job, err := p.SimulatedAllocator.GetImageJob(ctx, req.OwnerUID, jobs[i].ID)
			if err != nil {
				return err
			}
			complete = job.Done
		}
	}
	if !complete {
		return fmt.Errorf("joined host %s before image completion", req.HostID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.operations = append(p.operations, req)
	p.joined[req.HostID] = true
	if p.loseJoinResponse {
		p.loseJoinResponse = false
		return errors.New("join completion response lost")
	}
	return nil
}

func (p *lifecycleProvider) ValidateGPU(ctx context.Context, req lab.HostOperation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.waitForBlockedImage {
		select {
		case <-p.imageStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.joined[req.HostID] {
		return fmt.Errorf("validated host %s before joining", req.HostID)
	}
	p.operations = append(p.operations, req)
	if p.validationErr == nil {
		delete(p.activeHosts, req.HostID)
	}
	return p.validationErr
}

func (p *lifecycleProvider) ReleaseHosts(ctx context.Context, ownerUID string) error {
	p.mu.Lock()
	p.releaseCalls++
	err := p.cleanupErr
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.SimulatedAllocator.ReleaseHosts(ctx, ownerUID)
}

func (p *lifecycleProvider) GetImageJob(ctx context.Context, uid, jobID string) (lab.ImageJob, error) {
	job, err := p.SimulatedAllocator.GetImageJob(ctx, uid, jobID)
	if err != nil {
		return lab.ImageJob{}, err
	}
	if !p.blockImages && job.Request.HostID != p.blockHost {
		return p.faultProvider.GetImageJob(ctx, uid, jobID)
	}
	p.imageStarted <- struct{}{}
	<-ctx.Done()
	p.mu.Lock()
	p.canceledImages++
	p.mu.Unlock()
	p.imageStopped <- struct{}{}
	return lab.ImageJob{}, ctx.Err()
}

func (p *lifecycleProvider) EnsureHost(ctx context.Context, uid string, slot int) (lab.HostAllocation, error) {
	if slot == p.failAllocationSlot {
		return lab.HostAllocation{}, errors.New("slot capacity unavailable")
	}
	return p.faultProvider.EnsureHost(ctx, uid, slot)
}
