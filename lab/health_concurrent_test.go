package lab

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type batchOutcome struct {
	results []HostResult
	err     error
}

func TestCheckHostsUsesWorkersWithinLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan string, 5)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	var active, exceeded atomic.Int32
	check := func(_ context.Context, host string) error {
		if active.Add(1) > 2 {
			exceeded.Add(1)
		}
		defer active.Add(-1)
		started <- host
		<-release
		return nil
	}
	done := make(chan batchOutcome, 1)
	go func() {
		results, err := CheckHosts(ctx, []string{"a", "b", "c", "d", "e"}, 2, check)
		done <- batchOutcome{results, err}
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("two checks did not overlap despite limit=2")
		}
	}
	select {
	case host := <-started:
		t.Fatalf("third check %q started while both slots were occupied", host)
	case <-time.After(25 * time.Millisecond):
	}
	releaseAll()
	select {
	case outcome := <-done:
		if outcome.err != nil || len(outcome.results) != 5 {
			t.Fatalf("got %d results and error %v, want 5 and nil", len(outcome.results), outcome.err)
		}
		seen := make(map[string]bool)
		for _, result := range outcome.results {
			if seen[result.Host] || result.Err != nil {
				t.Errorf("duplicate or unsuccessful result: %+v", result)
			}
			seen[result.Host] = true
		}
		for _, host := range []string{"a", "b", "c", "d", "e"} {
			if !seen[host] {
				t.Errorf("missing result for %q", host)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("checker did not return after all checks were released")
	}
	if exceeded.Load() != 0 || active.Load() != 0 {
		t.Errorf("concurrency violations=%d, remaining active=%d", exceeded.Load(), active.Load())
	}
}

func TestCheckHostsCancellationDrainsStartedChecks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 2)
	var active atomic.Int32
	check := func(ctx context.Context, _ string) error {
		active.Add(1)
		defer active.Add(-1)
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	done := make(chan batchOutcome, 1)
	go func() {
		results, err := CheckHosts(ctx, []string{"a", "b", "queued"}, 2, check)
		done <- batchOutcome{results, err}
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("two checks did not start")
		}
	}
	cancel()
	select {
	case outcome := <-done:
		if !errors.Is(outcome.err, context.Canceled) || len(outcome.results) != 2 {
			t.Fatalf("got %d results and error %v, want 2 and context.Canceled", len(outcome.results), outcome.err)
		}
		for _, result := range outcome.results {
			if result.Host == "queued" || !errors.Is(result.Err, context.Canceled) {
				t.Errorf("unexpected result: %+v", result)
			}
		}
		if active.Load() != 0 {
			t.Errorf("returned with %d checks still running", active.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("checker did not finish after checks observed cancellation")
	}
}

func TestCheckHostsWaitsForUncooperativeCheck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCheck := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseCheck()
	done := make(chan batchOutcome, 1)
	go func() {
		results, err := CheckHosts(ctx, []string{"a", "queued"}, 1, func(context.Context, string) error {
			close(started)
			<-release // Deliberately ignores cancellation until the test releases it.
			return nil
		})
		done <- batchOutcome{results, err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("check did not start")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("returned while the underlying check was still running")
	case <-time.After(25 * time.Millisecond):
	}
	releaseCheck()
	select {
	case outcome := <-done:
		if !errors.Is(outcome.err, context.Canceled) || len(outcome.results) != 1 {
			t.Fatalf("got %d results and error %v, want 1 and context.Canceled", len(outcome.results), outcome.err)
		}
		if outcome.results[0].Host != "a" || outcome.results[0].Err != nil {
			t.Errorf("completed check result = %+v, want host a with nil error", outcome.results[0])
		}
	case <-time.After(time.Second):
		t.Fatal("checker did not return after underlying check finished")
	}
}

func TestCheckHostsRejectsInvalidLimit(t *testing.T) {
	for _, limit := range []int{0, -1} {
		var called atomic.Bool
		results, err := CheckHosts(context.Background(), []string{"a"}, limit, func(context.Context, string) error {
			called.Store(true)
			return nil
		})
		if !errors.Is(err, ErrInvalidLimit) || len(results) != 0 || called.Load() {
			t.Errorf("limit=%d: got %v, %v, called=%v; want no results, ErrInvalidLimit, no calls", limit, results, err, called.Load())
		}
	}
}
