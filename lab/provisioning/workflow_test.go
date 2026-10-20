package provisioning

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// Catches changing provider identity between activity attempts or relying on
// discovery alone: the retry's discovery deliberately hides the created host.
func TestProvisionClusterRetriesLostResponseWithStableIdentity(t *testing.T) {
	provider := &faultProvider{SimulatedAllocator: &lab.SimulatedAllocator{}, loseResponse: true}
	env, attempts := executeProvision(t, provider, Request{ClusterUID: "cluster-A", NodeCount: 3, OperationID: "revision-1", ImageVersion: "os-v1"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result []lab.HostAllocation
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || len(result) != 3 {
		t.Fatalf("activity attempts=%d result=%v; want 2 attempts and 3 hosts", attempts, result)
	}
	keys, firstCreated, _ := provider.snapshot()
	if len(keys) < 2 || keys[0] != (allocationCall{"cluster-A", 1}) || keys[1] != keys[0] {
		t.Fatalf("provider retry keys=%v; want the same cluster-A/slot-1 key", keys)
	}
	if result[0] != firstCreated {
		t.Fatalf("retry replaced original allocation: created=%+v result=%+v", firstCreated, result[0])
	}
	inventory, err := provider.SimulatedAllocator.ListHosts(t.Context(), "cluster-A")
	if err != nil || len(inventory) != 3 {
		t.Fatalf("inventory=%v, %v; want exactly 3 logical allocations", inventory, err)
	}
	for i, host := range result {
		if host.OwnerUID != "cluster-A" || host.Slot != i+1 || host != inventory[i] {
			t.Fatalf("unexpected allocation at index %d: %+v", i, host)
		}
	}
}

// Catches unbounded retrying or allocating after failed provider discovery.
func TestProvisionClusterBoundsTransientRetries(t *testing.T) {
	provider := &faultProvider{SimulatedAllocator: &lab.SimulatedAllocator{}, listErr: errors.New("provider unavailable")}
	env, attempts := executeProvision(t, provider, Request{ClusterUID: "cluster-A", NodeCount: 3, OperationID: "revision-1", ImageVersion: "os-v1"})
	if err := env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), "provider unavailable") {
		t.Fatalf("workflow error=%v; want provider failure", err)
	}
	if attempts != 3 {
		t.Fatalf("activity attempts=%d; want bounded total of 3", attempts)
	}
	keys, _, _ := provider.snapshot()
	if len(keys) != 0 {
		t.Fatalf("allocation attempted despite failed discovery: %v", keys)
	}
}

// Catches retrying permanent input errors or making provider calls before
// validating requests at the activity boundary.
func TestProvisionClusterRejectsInvalidRequests(t *testing.T) {
	for _, tc := range []struct {
		name    string
		request Request
	}{
		{"missing UID", Request{NodeCount: 3, OperationID: "revision-1", ImageVersion: "os-v1"}},
		{"negative count", Request{ClusterUID: "cluster-A", NodeCount: -1, OperationID: "revision-1", ImageVersion: "os-v1"}},
		{"above lab limit", Request{ClusterUID: "cluster-A", NodeCount: 101, OperationID: "revision-1", ImageVersion: "os-v1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &faultProvider{SimulatedAllocator: &lab.SimulatedAllocator{}}
			env, attempts := executeProvision(t, provider, tc.request)
			assertNonRetryable(t, env.GetWorkflowError())
			if attempts != 1 {
				t.Fatalf("invalid request attempts=%d; want 1", attempts)
			}
			keys, _, lists := provider.snapshot()
			if len(keys) != 0 || lists != 0 {
				t.Fatalf("invalid request called provider: lists=%d ensures=%v", lists, keys)
			}
		})
	}
}

// Catches late workflow work retrying forever or recreating hosts after the
// provider closed this cluster UID during cleanup.
func TestProvisionClusterDoesNotRetryReleasedOwner(t *testing.T) {
	provider := &faultProvider{SimulatedAllocator: &lab.SimulatedAllocator{}}
	if err := provider.ReleaseHosts(t.Context(), "cluster-A"); err != nil {
		t.Fatal(err)
	}
	env, attempts := executeProvision(t, provider, Request{ClusterUID: "cluster-A", NodeCount: 1, OperationID: "revision-1", ImageVersion: "os-v1"})
	assertNonRetryable(t, env.GetWorkflowError())
	if attempts != 1 {
		t.Fatalf("released UID attempts=%d; want 1", attempts)
	}
	hosts, err := provider.SimulatedAllocator.ListHosts(t.Context(), "cluster-A")
	if err != nil || len(hosts) != 0 {
		t.Fatalf("late activity recreated allocation: %v, %v", hosts, err)
	}
}

// Scale-down is unsupported: stop retrying and explicitly release this failed
// lifecycle's existing reservations.
func TestProvisionClusterRejectsScaleDown(t *testing.T) {
	provider := &faultProvider{SimulatedAllocator: &lab.SimulatedAllocator{}}
	_, err := provider.SimulatedAllocator.EnsureHost(t.Context(), "cluster-A", 2)
	if err != nil {
		t.Fatal(err)
	}

	env, attempts := executeProvision(t, provider, Request{ClusterUID: "cluster-A", NodeCount: 1, OperationID: "revision-1", ImageVersion: "os-v1"})
	assertNonRetryable(t, env.GetWorkflowError())
	if attempts != 1 {
		t.Fatalf("unsupported scale-down attempts=%d; want 1", attempts)
	}
	keys, _, _ := provider.snapshot()
	if len(keys) != 0 {
		t.Fatalf("scale-down attempted new allocations: %v", keys)
	}
	hosts, err := provider.SimulatedAllocator.ListHosts(t.Context(), "cluster-A")
	if err != nil || len(hosts) != 0 {
		t.Fatalf("failed lifecycle left reservations: %v, %v", hosts, err)
	}
}

// Catches skipping imaging, changing its key on retry, sharing a key between
// hosts, or repeating reservation because a later activity lost its response.
func TestProvisionClusterImagesReservedHostsAfterLostResponse(t *testing.T) {
	provider := &faultProvider{SimulatedAllocator: &lab.SimulatedAllocator{}, loseImageResponse: true}
	req := Request{ClusterUID: "cluster-A", NodeCount: 2, OperationID: "revision-1", ImageVersion: "os-v1"}
	env, reservations := executeProvision(t, provider, req)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var hosts []lab.HostAllocation
	if err := env.GetWorkflowResult(&hosts); err != nil {
		t.Fatal(err)
	}
	if reservations != 1 || len(hosts) != 2 {
		t.Fatalf("reservation attempts=%d hosts=%v; want 1 and 2", reservations, hosts)
	}
	requests, jobs := provider.imageSnapshot()
	first := lab.ImageRequest{OwnerUID: "cluster-A", OperationID: "revision-1/image/1", HostID: hosts[0].ID, ImageVersion: "os-v1"}
	second := lab.ImageRequest{OwnerUID: "cluster-A", OperationID: "revision-1/image/2", HostID: hosts[1].ID, ImageVersion: "os-v1"}
	if !reflect.DeepEqual(requests, []lab.ImageRequest{first, first, second}) {
		t.Fatalf("image requests=%+v; want first host retried unchanged, then second host", requests)
	}
	if len(jobs) != 3 || jobs[0].ID != jobs[1].ID || jobs[0].ID == jobs[2].ID {
		t.Fatalf("jobs=%+v; want one job per host, reused across retry", jobs)
	}
	for _, job := range []lab.ImageJob{jobs[0], jobs[2]} {
		observed, err := provider.SimulatedAllocator.GetImageJob(t.Context(), "cluster-A", job.ID)
		if err != nil || !observed.Done {
			t.Fatalf("workflow returned before imaging completed: %+v, %v", observed, err)
		}
	}
}

// Catches allocating capacity before finding out the image intent is invalid.
func TestProvisionClusterRejectsMissingImageIntentBeforeReservation(t *testing.T) {
	for _, req := range []Request{
		{ClusterUID: "cluster-A", NodeCount: 2, ImageVersion: "os-v1"},
		{ClusterUID: "cluster-A", NodeCount: 2, OperationID: "revision-1"},
	} {
		provider := &faultProvider{SimulatedAllocator: &lab.SimulatedAllocator{}}
		env, reservations := executeProvision(t, provider, req)
		assertNonRetryable(t, env.GetWorkflowError())
		keys, _, lists := provider.snapshot()
		if reservations != 0 || lists != 0 || len(keys) != 0 {
			t.Fatalf("invalid image intent touched reservation: attempts=%d lists=%d keys=%v", reservations, lists, keys)
		}
	}
}

// Catches treating a conflicting old job as success or proceeding to later
// hosts after a terminal image error. Accepted jobs survive explicit cleanup.
func TestProvisionClusterStopsOnConflictingImageIntent(t *testing.T) {
	provider := &faultProvider{SimulatedAllocator: &lab.SimulatedAllocator{}}
	host, err := provider.SimulatedAllocator.EnsureHost(t.Context(), "cluster-A", 1)
	if err != nil {
		t.Fatal(err)
	}
	original, err := provider.SimulatedAllocator.EnsureImageJob(t.Context(), lab.ImageRequest{
		OwnerUID: "cluster-A", OperationID: "revision-1/image/1", HostID: host.ID, ImageVersion: "os-v0",
	})
	if err != nil {
		t.Fatal(err)
	}
	env, reservations := executeProvision(t, provider, Request{ClusterUID: "cluster-A", NodeCount: 2, OperationID: "revision-1", ImageVersion: "os-v1"})
	assertNonRetryable(t, env.GetWorkflowError())
	requests, _ := provider.imageSnapshot()
	if reservations != 1 || len(requests) != 1 || requests[0].HostID != host.ID {
		t.Fatalf("reservation attempts=%d image requests=%+v; want one terminal imaging attempt", reservations, requests)
	}
	// Cleanup fences observations/new work; an already accepted external image
	// job remains and can still complete after owner release.
	observed, err := provider.SimulatedAllocator.CompleteImageJob(t.Context(), "cluster-A", original.ID)
	if err != nil || observed.ID != original.ID || observed.Request != original.Request {
		t.Fatalf("conflict discarded original job: %+v, %v", observed, err)
	}
	hosts, err := provider.SimulatedAllocator.ListHosts(t.Context(), "cluster-A")
	if err != nil || len(hosts) != 0 {
		t.Fatalf("failed imaging left reservations: %v, %v", hosts, err)
	}
}

func executeProvision(t *testing.T, provider *faultProvider, req Request) (*testsuite.TestWorkflowEnvironment, int) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(3 * time.Second)
	env.RegisterWorkflow(ProvisionCluster)
	activities := &Activities{Provider: provider}
	env.RegisterActivityWithOptions(activities.ReserveHosts, activity.RegisterOptions{Name: ReserveHostsActivity})
	imaging := &ImagingActivities{Provider: provider, PollInterval: time.Millisecond}
	env.RegisterActivityWithOptions(imaging.EnsureImage, activity.RegisterOptions{Name: EnsureImageActivity})
	lifecycle := &LifecycleActivities{Provider: &lifecycleProvider{faultProvider: provider, joined: make(map[string]bool)}}
	env.RegisterActivityWithOptions(lifecycle.JoinCluster, activity.RegisterOptions{Name: JoinClusterActivity})
	env.RegisterActivityWithOptions(lifecycle.ValidateGPU, activity.RegisterOptions{Name: ValidateGPUActivity})
	env.RegisterActivityWithOptions(lifecycle.ReleaseHosts, activity.RegisterOptions{Name: ReleaseHostsActivity})
	var attempts atomic.Int32
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		if info.ActivityType.Name == ReserveHostsActivity {
			attempts.Add(1)
		}
	})
	env.ExecuteWorkflow(ProvisionCluster, req)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete in test environment")
	}
	return env, int(attempts.Load())
}

func assertNonRetryable(t *testing.T, err error) {
	t.Helper()
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Fatalf("workflow error=%v; want non-retryable application error", err)
	}
}

type allocationCall struct {
	uid  string
	slot int
}

// The provider is real; only its responses are faulted. State stays alive
// across attempts. This models a lost provider response, not a worker restart.
type faultProvider struct {
	*lab.SimulatedAllocator
	mu                sync.Mutex
	listErr           error
	loseResponse      bool
	hideNextList      bool
	listCalls         int
	keys              []allocationCall
	firstCreated      lab.HostAllocation
	loseImageResponse bool
	imageRequests     []lab.ImageRequest
	imageJobs         []lab.ImageJob
}

func (p *faultProvider) EnsureImageJob(ctx context.Context, req lab.ImageRequest) (lab.ImageJob, error) {
	job, err := p.SimulatedAllocator.EnsureImageJob(ctx, req)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.imageRequests = append(p.imageRequests, req)
	p.imageJobs = append(p.imageJobs, job)
	if err == nil && p.loseImageResponse {
		p.loseImageResponse = false
		return lab.ImageJob{}, errors.New("image creation response lost")
	}
	return job, err
}

// Tests supply a completion event on the first read of each job. The production
// provider does not complete jobs automatically when polled.
func (p *faultProvider) GetImageJob(ctx context.Context, uid, jobID string) (lab.ImageJob, error) {
	if _, err := p.SimulatedAllocator.CompleteImageJob(ctx, uid, jobID); err != nil {
		return lab.ImageJob{}, err
	}
	return p.SimulatedAllocator.GetImageJob(ctx, uid, jobID)
}

func (p *faultProvider) imageSnapshot() ([]lab.ImageRequest, []lab.ImageJob) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]lab.ImageRequest(nil), p.imageRequests...), append([]lab.ImageJob(nil), p.imageJobs...)
}

func (p *faultProvider) ListHosts(ctx context.Context, uid string) ([]lab.HostAllocation, error) {
	p.mu.Lock()
	p.listCalls++
	err := p.listErr
	hide := p.hideNextList
	p.hideNextList = false
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if hide {
		return nil, nil
	}
	return p.SimulatedAllocator.ListHosts(ctx, uid)
}

func (p *faultProvider) EnsureHost(ctx context.Context, uid string, slot int) (lab.HostAllocation, error) {
	host, err := p.SimulatedAllocator.EnsureHost(ctx, uid, slot)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys = append(p.keys, allocationCall{uid, slot})
	if err == nil && p.loseResponse {
		p.loseResponse = false
		p.hideNextList = true
		p.firstCreated = host
		return lab.HostAllocation{}, errors.New("provider completion response lost")
	}
	return host, err
}

func (p *faultProvider) snapshot() ([]allocationCall, lab.HostAllocation, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]allocationCall(nil), p.keys...), p.firstCreated, p.listCalls
}
