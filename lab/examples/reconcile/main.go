package main

import (
	"context"
	"fmt"
	"log"

	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
)

func main() {
	ctx := context.Background()
	provider := &lab.SimulatedAllocator{}
	const uid = "training-uid-A"
	for slot := 1; slot <= 2; slot++ {
		if _, err := provider.EnsureHost(ctx, uid, slot); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Desired: 3 hosts. Provider initially owns 2 for this cluster.")

	// Discard the report to simulate a controller crash before status is saved.
	if _, err := lab.ReconcileHosts(ctx, provider, uid, 3); err != nil {
		log.Fatal(err)
	}
	created, err := provider.ListHosts(ctx, uid)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("After allocation, before saved status: %d hosts. Controller report discarded.\n", len(created))

	// The provider survives the simulated controller restart.
	report, err := lab.ReconcileHosts(ctx, provider, uid, 3)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Next reconciliation reports %d hosts:\n", len(report))
	for _, host := range report {
		fmt.Printf("  slot %d: %s (owner %s)\n", host.Slot, host.ID, host.OwnerUID)
	}
	actual, err := provider.ListHosts(ctx, uid)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Provider inventory after retry: %d hosts.\n", len(actual))

	// This is the provider operation a future finalizer branch will call.
	for range 2 {
		if err := provider.ReleaseHosts(ctx, uid); err != nil {
			log.Fatal(err)
		}
	}
	remaining, err := provider.ListHosts(ctx, uid)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("After cleanup and its retry: %d hosts.\n", len(remaining))
	_, lateErr := provider.EnsureHost(ctx, uid, 3)
	fmt.Printf("Delayed allocation after cleanup: %v\n", lateErr)
}
