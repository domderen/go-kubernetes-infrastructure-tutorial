package lab

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	ErrInvalidAllocation    = errors.New("owner UID and positive host slot are required")
	ErrInvalidDesiredHosts  = errors.New("owner UID and nonnegative desired host count are required")
	ErrScaleDownUnsupported = errors.New("scale-down is not implemented in this lesson")
	ErrOwnerReleased        = errors.New("cluster allocation has been released")
)

// HostAllocation records provider-owned identity, independently of health.
// Slot is a stable logical position within a cluster, starting at one.
type HostAllocation struct {
	ID       string
	OwnerUID string
	Slot     int
}

// HostAllocator is the provider boundary used by the allocation reconciler.
// EnsureHost must atomically return the same host for a repeated owner/slot key.
type HostAllocator interface {
	ListHosts(context.Context, string) ([]HostAllocation, error)
	EnsureHost(context.Context, string, int) (HostAllocation, error)
}

// SimulatedAllocator holds in-memory provider state for the lab.
// Keep it alive across simulated controller/activity restarts; it is not durable.
type SimulatedAllocator struct {
	mu             sync.Mutex
	hosts          map[allocationKey]HostAllocation
	releasedOwners map[string]bool
	nextID         uint64
	imageJobs      map[imageOperationKey]ImageJob
	nextImageID    uint64
}

type allocationKey struct {
	ownerUID string
	slot     int
}

// ListHosts returns a snapshot sorted by slot for this owner alone.
func (p *SimulatedAllocator) ListHosts(ctx context.Context, ownerUID string) ([]HostAllocation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var hosts []HostAllocation
	for key, host := range p.hosts {
		if key.ownerUID == ownerUID {
			hosts = append(hosts, host)
		}
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].Slot < hosts[j].Slot })
	return hosts, nil
}

// EnsureHost atomically finds or creates an allocation for an owner/slot key.
// A retry returns the existing host, including after a lost response.
func (p *SimulatedAllocator) EnsureHost(ctx context.Context, ownerUID string, slot int) (HostAllocation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return HostAllocation{}, err
	}
	if ownerUID == "" || slot < 1 {
		return HostAllocation{}, ErrInvalidAllocation
	}
	if p.releasedOwners[ownerUID] {
		return HostAllocation{}, ErrOwnerReleased
	}
	key := allocationKey{ownerUID: ownerUID, slot: slot}
	if host, found := p.hosts[key]; found {
		return host, nil
	}
	if p.hosts == nil {
		p.hosts = make(map[allocationKey]HostAllocation)
	}
	p.nextID++
	host := HostAllocation{
		ID:       fmt.Sprintf("host-%03d", p.nextID),
		OwnerUID: ownerUID,
		Slot:     slot,
	}
	p.hosts[key] = host
	return host, nil
}

// ReleaseHosts fulfills the cleanup obligation for one cluster UID.
// Repeated cleanup succeeds, while future allocations for that UID are rejected.
func (p *SimulatedAllocator) ReleaseHosts(ctx context.Context, ownerUID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if ownerUID == "" {
		return errors.New("owner UID is required for cleanup")
	}
	if p.releasedOwners == nil {
		p.releasedOwners = make(map[string]bool)
	}
	// Closing this owner and deleting its hosts share the same lock as ensure.
	// A request arriving after cleanup cannot recreate an orphaned allocation.
	p.releasedOwners[ownerUID] = true
	for key := range p.hosts {
		if key.ownerUID == ownerUID {
			delete(p.hosts, key)
		}
	}
	return nil
}
