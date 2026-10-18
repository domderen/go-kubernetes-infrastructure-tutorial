package lab

import (
	"context"
	"errors"
	"testing"
)

// Catches creation based on stale status rather than provider discovery.
func TestReconcileHostsRecoversWithoutSavedStatus(t *testing.T) {
	ctx := context.Background()
	provider := &SimulatedAllocator{}
	for slot := 1; slot <= 2; slot++ {
		if _, err := provider.EnsureHost(ctx, "uid-A", slot); err != nil {
			t.Fatal(err)
		}
	}
	// The first pass allocates the third host; discard its report as if the
	// controller crashed before saving Kubernetes status.
	if _, err := ReconcileHosts(ctx, provider, "uid-A", 3); err != nil {
		t.Fatal(err)
	}
	before, err := provider.ListHosts(ctx, "uid-A")
	if err != nil || len(before) != 3 {
		t.Fatalf("provider after first pass = %v, %v; want 3 hosts", before, err)
	}
	recovered, err := ReconcileHosts(ctx, provider, "uid-A", 3)
	if err != nil || len(recovered) != 3 {
		t.Fatalf("recovered report = %v, %v; want 3 hosts", recovered, err)
	}
	for i, host := range recovered {
		if host != before[i] {
			t.Errorf("host changed on retry: before=%+v after=%+v", before[i], host)
		}
	}
	after, err := provider.ListHosts(ctx, "uid-A")
	if err != nil || len(after) != 3 {
		t.Fatalf("provider after retry = %v, %v; want 3 hosts", after, err)
	}
}

type failingAllocator struct {
	HostAllocator
	listErr, lostResponse error
}

func (p *failingAllocator) ListHosts(ctx context.Context, uid string) ([]HostAllocation, error) {
	if p.listErr != nil {
		return nil, p.listErr
	}
	return p.HostAllocator.ListHosts(ctx, uid)
}

func (p *failingAllocator) EnsureHost(ctx context.Context, uid string, slot int) (HostAllocation, error) {
	host, err := p.HostAllocator.EnsureHost(ctx, uid, slot)
	if err == nil && p.lostResponse != nil {
		err = p.lostResponse
		p.lostResponse = nil
		return HostAllocation{}, err
	}
	return host, err
}

// Catches allocating a replacement after the provider created a host but its
// response was lost. The wrapper injects failure after a real side effect.
func TestReconcileHostsRecoversFromLostResponse(t *testing.T) {
	ctx := context.Background()
	provider := &SimulatedAllocator{}
	lost := errors.New("allocation response lost")
	fault := &failingAllocator{HostAllocator: provider, lostResponse: lost}
	if _, err := ReconcileHosts(ctx, fault, "uid-A", 3); !errors.Is(err, lost) {
		t.Fatalf("first pass error = %v, want lost response", err)
	}
	created, err := provider.ListHosts(ctx, "uid-A")
	if err != nil || len(created) != 1 {
		t.Fatalf("inventory after lost response = %v, %v; want 1 host", created, err)
	}
	recovered, err := ReconcileHosts(ctx, fault, "uid-A", 3)
	if err != nil || len(recovered) != 3 || recovered[0] != created[0] {
		t.Fatalf("recovered report = %v, %v; want 3 including original host", recovered, err)
	}
}

// Catches treating an unavailable provider as an empty inventory.
func TestReconcileHostsDoesNotAllocateWhenDiscoveryFails(t *testing.T) {
	provider := &SimulatedAllocator{}
	unavailable := errors.New("provider unavailable")
	fault := &failingAllocator{HostAllocator: provider, listErr: unavailable}
	if _, err := ReconcileHosts(context.Background(), fault, "uid-A", 3); !errors.Is(err, unavailable) {
		t.Fatalf("error = %v, want provider unavailable", err)
	}
	hosts, err := provider.ListHosts(context.Background(), "uid-A")
	if err != nil || len(hosts) != 0 {
		t.Fatalf("allocated without discovery: %v, %v", hosts, err)
	}
}

// Catches silently declaring success for unsupported scale-down or bad input.
func TestReconcileHostsRejectsUnsupportedRequests(t *testing.T) {
	provider := &SimulatedAllocator{}
	ctx := context.Background()
	for _, tc := range []struct {
		uid     string
		desired int
	}{
		{"", 3}, {"uid-A", -1},
	} {
		if _, err := ReconcileHosts(ctx, provider, tc.uid, tc.desired); !errors.Is(err, ErrInvalidDesiredHosts) {
			t.Errorf("uid=%q desired=%d: got %v, want invalid request", tc.uid, tc.desired, err)
		}
	}
	if _, err := provider.EnsureHost(ctx, "uid-A", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := ReconcileHosts(ctx, provider, "uid-A", 1); !errors.Is(err, ErrScaleDownUnsupported) {
		t.Fatalf("scale-down error = %v, want unsupported", err)
	}
	hosts, err := provider.ListHosts(ctx, "uid-A")
	if err != nil || len(hosts) != 1 || hosts[0].Slot != 2 {
		t.Fatalf("unsupported request changed inventory: %v, %v", hosts, err)
	}
}
