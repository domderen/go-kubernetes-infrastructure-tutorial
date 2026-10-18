package lab

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// Catches deleting another owner's hosts or failing a retry after a lost
// cleanup response. The first successful response is deliberately discarded.
func TestReleaseHostsIsOwnerScopedAndRepeatable(t *testing.T) {
	ctx := context.Background()
	provider := &SimulatedAllocator{}
	for _, uid := range []string{"uid-A", "uid-B"} {
		if _, err := ReconcileHosts(ctx, provider, uid, 3); err != nil {
			t.Fatal(err)
		}
	}
	beforeB, err := provider.ListHosts(ctx, "uid-B")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := provider.ReleaseHosts(ctx, "uid-A"); err != nil {
			t.Fatalf("cleanup should be repeatable: %v", err)
		}
	}
	a, err := provider.ListHosts(ctx, "uid-A")
	if err != nil || len(a) != 0 {
		t.Fatalf("A inventory after cleanup = %v, %v; want empty", a, err)
	}
	b, err := provider.ListHosts(ctx, "uid-B")
	if err != nil || len(b) != 3 {
		t.Fatalf("B inventory = %v, %v; want 3 hosts", b, err)
	}
	for i, host := range b {
		if host != beforeB[i] {
			t.Errorf("cleanup changed B's host: before=%+v after=%+v", beforeB[i], host)
		}
	}
}

// Catches late work recreating resources after cleanup completed, including
// cleanup of an owner whose first allocation request had not arrived yet.
func TestReleaseHostsRejectsLateAllocations(t *testing.T) {
	ctx := context.Background()
	provider := &SimulatedAllocator{}
	if err := provider.ReleaseHosts(ctx, "uid-A"); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.EnsureHost(ctx, "uid-A", 1); !errors.Is(err, ErrOwnerReleased) {
		t.Fatalf("late allocation error = %v, want ErrOwnerReleased", err)
	}
	hosts, err := provider.ListHosts(ctx, "uid-A")
	if err != nil || len(hosts) != 0 {
		t.Fatalf("late work recreated resources: %v, %v", hosts, err)
	}
}

// Catches a release/create race leaving hosts after the release acknowledged.
func TestReleaseHostsFencesConcurrentAllocations(t *testing.T) {
	ctx := context.Background()
	provider := &SimulatedAllocator{}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for slot := 1; slot <= 20; slot++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			if _, err := provider.EnsureHost(ctx, "uid-A", slot); err != nil && !errors.Is(err, ErrOwnerReleased) {
				t.Errorf("ensure: %v", err)
			}
		}()
	}
	close(start)
	if err := provider.ReleaseHosts(ctx, "uid-A"); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	hosts, err := provider.ListHosts(ctx, "uid-A")
	if err != nil || len(hosts) != 0 {
		t.Fatalf("resources survived cleanup: %v, %v", hosts, err)
	}
}

// Catches reporting successful cleanup or deleting hosts with a canceled call.
func TestReleaseHostsCanceledRequestKeepsAllocation(t *testing.T) {
	provider := &SimulatedAllocator{}
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := provider.EnsureHost(ctx, "uid-A", 1); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := provider.ReleaseHosts(ctx, "uid-A"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cleanup error = %v, want context.Canceled", err)
	}
	hosts, err := provider.ListHosts(context.Background(), "uid-A")
	if err != nil || len(hosts) != 1 {
		t.Fatalf("canceled cleanup changed inventory: %v, %v", hosts, err)
	}
}

func TestReleaseHostsRejectsEmptyOwner(t *testing.T) {
	provider := &SimulatedAllocator{}
	ctx := context.Background()
	if _, err := provider.EnsureHost(ctx, "uid-A", 1); err != nil {
		t.Fatal(err)
	}
	if err := provider.ReleaseHosts(ctx, ""); err == nil {
		t.Fatal("cleanup accepted an empty owner UID")
	}
	hosts, err := provider.ListHosts(ctx, "uid-A")
	if err != nil || len(hosts) != 1 {
		t.Fatalf("invalid cleanup changed inventory: %v, %v", hosts, err)
	}
}
