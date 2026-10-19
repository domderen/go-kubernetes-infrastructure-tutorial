package provisioning

import (
	"context"
	"errors"
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
	env, attempts := executeProvision(t, provider, Request{ClusterUID: "cluster-A", NodeCount: 3})
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
	env, attempts := executeProvision(t, provider, Request{ClusterUID: "cluster-A", NodeCount: 3})
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
		{"missing UID", Request{NodeCount: 3}},
		{"negative count", Request{ClusterUID: "cluster-A", NodeCount: -1}},
		{"above lab limit", Request{ClusterUID: "cluster-A", NodeCount: 101}},
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
	env, attempts := executeProvision(t, provider, Request{ClusterUID: "cluster-A", NodeCount: 1})
	assertNonRetryable(t, env.GetWorkflowError())
	if attempts != 1 {
		t.Fatalf("released UID attempts=%d; want 1", attempts)
	}
	hosts, err := provider.SimulatedAllocator.ListHosts(t.Context(), "cluster-A")
	if err != nil || len(hosts) != 0 {
		t.Fatalf("late activity recreated allocation: %v, %v", hosts, err)
	}
}

// Scale-down is intentionally unsupported: retries must not alter the existing
// reservation or keep retrying a request the provider cannot fulfill.
func TestProvisionClusterRejectsScaleDown(t *testing.T) {
	provider := &faultProvider{SimulatedAllocator: &lab.SimulatedAllocator{}}
	existing, err := provider.SimulatedAllocator.EnsureHost(t.Context(), "cluster-A", 2)
	if err != nil {
		t.Fatal(err)
	}

	env, attempts := executeProvision(t, provider, Request{ClusterUID: "cluster-A", NodeCount: 1})
	assertNonRetryable(t, env.GetWorkflowError())
	if attempts != 1 {
		t.Fatalf("unsupported scale-down attempts=%d; want 1", attempts)
	}
	keys, _, _ := provider.snapshot()
	if len(keys) != 0 {
		t.Fatalf("scale-down attempted new allocations: %v", keys)
	}
	hosts, err := provider.SimulatedAllocator.ListHosts(t.Context(), "cluster-A")
	if err != nil || len(hosts) != 1 || hosts[0] != existing {
		t.Fatalf("scale-down changed inventory: %v, %v; want %v", hosts, err, existing)
	}
}

func executeProvision(t *testing.T, provider lab.HostAllocator, req Request) (*testsuite.TestWorkflowEnvironment, int) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(3 * time.Second)
	env.RegisterWorkflow(ProvisionCluster)
	activities := &Activities{Provider: provider}
	env.RegisterActivityWithOptions(activities.ReserveHosts, activity.RegisterOptions{Name: ReserveHostsActivity})
	var attempts atomic.Int32
	env.SetOnActivityStartedListener(func(*activity.Info, context.Context, converter.EncodedValues) { attempts.Add(1) })
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
	mu           sync.Mutex
	listErr      error
	loseResponse bool
	hideNextList bool
	listCalls    int
	keys         []allocationCall
	firstCreated lab.HostAllocation
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
