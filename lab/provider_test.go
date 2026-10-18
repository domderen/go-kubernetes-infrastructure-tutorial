package lab

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Catches dropped provider errors or a host-local timeout aborting the batch.
func TestSimulatedProviderFleetScan(t *testing.T) {
	unhealthy := errors.New("GPU diagnostic failed")
	provider := NewSimulatedProvider(map[string]SimulatedHost{
		"healthy":   {},
		"unhealthy": {HealthError: unhealthy},
		"slow":      {Delay: time.Hour},
	})

	results, err := CheckHosts(context.Background(), []string{"healthy", "unhealthy", "slow"}, 2,
		func(ctx context.Context, host string) error {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
			defer cancel()
			return provider.CheckHealth(ctx, host)
		})
	if err != nil {
		t.Fatalf("batch error = %v, want nil despite individual failures", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	want := map[string]error{
		"healthy": nil, "unhealthy": unhealthy, "slow": context.DeadlineExceeded,
	}
	for _, result := range results {
		wantErr, known := want[result.Host]
		if !known {
			t.Fatalf("unexpected or repeated host %q", result.Host)
		}
		if !errors.Is(result.Err, wantErr) {
			t.Errorf("host %q error = %v, want %v", result.Host, result.Err, wantErr)
		}
		delete(want, result.Host)
	}
}

// Catches a missing host being silently reported as healthy.
func TestSimulatedProviderUnknownHost(t *testing.T) {
	provider := NewSimulatedProvider(nil)
	if err := provider.CheckHealth(context.Background(), "missing"); !errors.Is(err, ErrHostNotFound) {
		t.Fatalf("error = %v, want ErrHostNotFound", err)
	}
}

// Catches replacing cooperative waiting with a sleep that ignores cancellation.
func TestSimulatedProviderCancellationInterruptsDelay(t *testing.T) {
	provider := NewSimulatedProvider(map[string]SimulatedHost{
		"slow": {Delay: time.Hour},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- provider.CheckHealth(ctx, "slow") }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("provider did not stop waiting after cancellation")
	}
}

// Catches reporting success when the request was canceled before starting.
func TestSimulatedProviderAlreadyCanceled(t *testing.T) {
	provider := NewSimulatedProvider(map[string]SimulatedHost{"healthy": {}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := provider.CheckHealth(ctx, "healthy"); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

// Catches retaining the caller's mutable configuration map.
func TestSimulatedProviderOwnsConfiguration(t *testing.T) {
	unhealthy := errors.New("GPU diagnostic failed")
	hosts := map[string]SimulatedHost{"gpu-01": {HealthError: unhealthy}}
	provider := NewSimulatedProvider(hosts)
	hosts["gpu-01"] = SimulatedHost{}
	delete(hosts, "gpu-01")
	if err := provider.CheckHealth(context.Background(), "gpu-01"); !errors.Is(err, unhealthy) {
		t.Fatalf("error = %v, want original health failure %v", err, unhealthy)
	}
}
