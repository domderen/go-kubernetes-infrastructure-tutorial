package lab

import (
	"context"
	"errors"
	"sync"
)

// ErrInvalidLimit means no positive concurrency limit was supplied.
var ErrInvalidLimit = errors.New("concurrency limit must be positive")

// CheckHosts runs at most limit checks at once. Results are returned in arrival
// order, which may differ from input order. Per-host errors do not abort the batch.
// Workers stop starting checks once they observe cancellation. Every started
// check is allowed to return before CheckHosts returns partial results and ctx.Err().
// The supplied check must be non-nil and honor ctx for prompt cancellation.
func CheckHosts(
	ctx context.Context,
	hosts []string,
	limit int,
	check func(context.Context, string) error,
) ([]HostResult, error) {
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	if len(hosts) == 0 || ctx.Err() != nil {
		return nil, ctx.Err()
	}

	jobs := make(chan string)
	outcomes := make(chan HostResult)
	var workers sync.WaitGroup
	workerCount := min(limit, len(hosts))
	workers.Add(workerCount)

	for range workerCount {
		go func() {
			defer workers.Done()
			for host := range jobs {
				if ctx.Err() != nil {
					return
				}

				checkErr := check(ctx, host)
				// The collector keeps receiving even after cancellation, so every
				// started check can report its outcome without abandoning work.
				outcomes <- HostResult{Host: host, Err: checkErr}
			}
		}()
	}

	go func() {
		dispatchHosts(ctx, hosts, jobs)
		workers.Wait()
		close(outcomes)
	}()

	// This goroutine is the sole owner of the results slice.
	var results []HostResult
	for result := range outcomes {
		results = append(results, result)
	}

	return results, ctx.Err()
}

func dispatchHosts(ctx context.Context, hosts []string, jobs chan<- string) {
	defer close(jobs)
	for _, host := range hosts {
		select {
		case <-ctx.Done():
			return
		case jobs <- host:
		}
	}
}
