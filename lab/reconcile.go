package lab

import (
	"context"
	"fmt"
)

// ReconcileHosts performs one allocation pass. Returned allocations can later
// be reported in Kubernetes status; previous status is not its source of truth.
// This is controller business logic, not yet a Kubernetes controller adapter.
func ReconcileHosts(ctx context.Context, provider HostAllocator, ownerUID string, desired int) ([]HostAllocation, error) {
	if ownerUID == "" || desired < 0 {
		return nil, ErrInvalidDesiredHosts
	}
	hosts, err := provider.ListHosts(ctx, ownerUID)
	if err != nil {
		return nil, fmt.Errorf("observe allocations: %w", err)
	}
	bySlot := make(map[int]HostAllocation, len(hosts))
	for _, host := range hosts {
		if host.Slot > desired {
			return hosts, ErrScaleDownUnsupported
		}
		bySlot[host.Slot] = host
	}
	var report []HostAllocation
	for slot := 1; slot <= desired; slot++ {
		host, found := bySlot[slot]
		if !found {
			host, err = provider.EnsureHost(ctx, ownerUID, slot)
			if err != nil {
				return report, fmt.Errorf("ensure slot %d: %w", slot, err)
			}
		}
		report = append(report, host)
	}
	return report, nil
}
