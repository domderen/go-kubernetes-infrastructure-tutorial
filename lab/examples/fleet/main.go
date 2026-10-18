package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
)

func main() {
	limit := flag.Int("limit", 2, "maximum simultaneous health checks")
	hostTimeout := flag.Duration("host-timeout", 250*time.Millisecond, "timeout for each host check")
	batchTimeout := flag.Duration("batch-timeout", 2*time.Second, "timeout for the entire fleet scan")
	flag.Parse()

	provider := lab.NewSimulatedProvider(map[string]lab.SimulatedHost{
		"gpu-01": {Delay: 50 * time.Millisecond},
		"gpu-02": {Delay: 100 * time.Millisecond, HealthError: errors.New("GPU diagnostic failed")},
		"gpu-03": {Delay: 2 * time.Second},
		"gpu-04": {Delay: 50 * time.Millisecond},
	})
	hosts := []string{"gpu-01", "gpu-02", "gpu-03", "gpu-04"}
	ctx, cancel := context.WithTimeout(context.Background(), *batchTimeout)
	defer cancel()

	fmt.Printf("Fleet: %d hosts; concurrency=%d; host timeout=%s; batch timeout=%s\n",
		len(hosts), *limit, *hostTimeout, *batchTimeout)
	started := time.Now()
	results, batchErr := lab.CheckHosts(ctx, hosts, *limit,
		func(ctx context.Context, host string) error {
			hostCtx, cancel := context.WithTimeout(ctx, *hostTimeout)
			defer cancel()
			return provider.CheckHealth(hostCtx, host)
		})

	// CheckHosts returns arrival order. Print only after the scan has drained.
	for _, result := range results {
		switch {
		case result.Err == nil:
			fmt.Printf("  %s: healthy\n", result.Host)
		case errors.Is(result.Err, context.DeadlineExceeded), errors.Is(result.Err, context.Canceled):
			fmt.Printf("  %s: health unknown (%v)\n", result.Host, result.Err)
		default:
			fmt.Printf("  %s: check failed (%v)\n", result.Host, result.Err)
		}
	}
	fmt.Printf("Returned %d/%d host results in %s; batch error: %v\n",
		len(results), len(hosts), time.Since(started).Round(time.Millisecond), batchErr)
}
