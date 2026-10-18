package lab

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// Catches duplicate allocations when the same operation is requested concurrently.
func TestEnsureHostIsIdempotent(t *testing.T) {
	provider := &SimulatedAllocator{}
	ctx := context.Background()
	var workers sync.WaitGroup
	results := make([]HostAllocation, 20)
	for i := range results {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var err error
			results[i], err = provider.EnsureHost(ctx, "uid-A", 1)
			if err != nil {
				t.Errorf("ensure: %v", err)
			}
		}()
	}
	workers.Wait()
	hosts, err := provider.ListHosts(ctx, "uid-A")
	if err != nil || len(hosts) != 1 {
		t.Fatalf("inventory = %v, %v; want one allocation", hosts, err)
	}
	for _, host := range results {
		if host != hosts[0] || host.ID == "" || host.OwnerUID != "uid-A" || host.Slot != 1 {
			t.Errorf("ensure returned unexpected allocation: %+v", host)
		}
	}
}

// Catches discovery treating different cluster incarnations as the same owner.
func TestAllocationsAreIsolatedByUID(t *testing.T) {
	provider := &SimulatedAllocator{}
	ctx := context.Background()
	a, err := provider.EnsureHost(ctx, "uid-A", 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := provider.EnsureHost(ctx, "uid-B", 1)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID == b.ID || a.OwnerUID != "uid-A" || b.OwnerUID != "uid-B" {
		t.Fatalf("ownership mixed: A=%+v, B=%+v", a, b)
	}
	hosts, err := provider.ListHosts(ctx, "uid-B")
	if err != nil || len(hosts) != 1 || hosts[0] != b {
		t.Fatalf("B inventory = %v, %v; want only B's host", hosts, err)
	}
}

// Catches invalid/canceled requests creating resources as a side effect.
func TestEnsureHostRejectsInvalidAndCanceledRequests(t *testing.T) {
	for _, tc := range []struct {
		name, uid string
		slot      int
		canceled  bool
		want      error
	}{
		{name: "empty UID", slot: 1, want: ErrInvalidAllocation},
		{name: "invalid slot", uid: "uid-A", slot: 0, want: ErrInvalidAllocation},
		{name: "canceled", uid: "uid-A", slot: 1, canceled: true, want: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &SimulatedAllocator{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			if _, err := provider.EnsureHost(ctx, tc.uid, tc.slot); !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			hosts, err := provider.ListHosts(context.Background(), "uid-A")
			if err != nil || len(hosts) != 0 {
				t.Fatalf("unexpected side effect: %v, %v", hosts, err)
			}
		})
	}
}
