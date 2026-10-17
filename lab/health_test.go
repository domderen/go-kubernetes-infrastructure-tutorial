package lab

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// One worker makes the concurrent implementation deterministic in shared tests
// that assert input order. Parallelism is covered in health_concurrent_test.go.
var checkerImplementations = []struct {
	name string
	run  func(context.Context, []string, func(context.Context, string) error) ([]HostResult, error)
}{
	{name: "sequential", run: CheckHostsSequential},
	{name: "concurrent", run: func(ctx context.Context, hosts []string, check func(context.Context, string) error) ([]HostResult, error) {
		return CheckHosts(ctx, hosts, 1, check)
	}},
}

func TestCheckHostsKeepsMixedResults(t *testing.T) {
	for _, implementation := range checkerImplementations {
		t.Run(implementation.name, func(t *testing.T) {
			unhealthy := errors.New("host reported unhealthy")
			var checked []string
			check := func(_ context.Context, host string) error {
				checked = append(checked, host)
				if host == "gpu-02" {
					return unhealthy
				}
				return nil
			}

			results, err := implementation.run(context.Background(), []string{"gpu-01", "gpu-02", "gpu-03"}, check)
			if err != nil {
				t.Fatalf("batch error = %v, want nil", err)
			}
			if !slices.Equal(checked, []string{"gpu-01", "gpu-02", "gpu-03"}) {
				t.Fatalf("checked hosts = %v, want all three in order", checked)
			}
			if len(results) != 3 {
				t.Fatalf("result count = %d, want 3", len(results))
			}
			for i, host := range []string{"gpu-01", "gpu-02", "gpu-03"} {
				if results[i].Host != host {
					t.Errorf("result %d host = %q, want %q", i, results[i].Host, host)
				}
			}
			if results[0].Err != nil || results[2].Err != nil {
				t.Errorf("healthy hosts have errors: first=%v, third=%v", results[0].Err, results[2].Err)
			}
			if !errors.Is(results[1].Err, unhealthy) {
				t.Errorf("unhealthy host error = %v, want %v", results[1].Err, unhealthy)
			}
		})
	}
}

func TestCheckHostsDoesNotStartWithCanceledContext(t *testing.T) {
	for _, implementation := range checkerImplementations {
		t.Run(implementation.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			results, err := implementation.run(ctx, []string{"gpu-01"}, func(context.Context, string) error {
				t.Fatal("started a host check with an already canceled context")
				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Errorf("batch error = %v, want context.Canceled", err)
			}
			if len(results) != 0 {
				t.Errorf("results = %v, want no attempted hosts", results)
			}
		})
	}
}

func TestCheckHostsReturnsPartialResultsOnCancellation(t *testing.T) {
	for _, implementation := range checkerImplementations {
		t.Run(implementation.name, func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				cancelHost string
				wantHosts  []string
			}{
				{"during middle check", "gpu-02", []string{"gpu-01", "gpu-02"}},
				{"during final check", "gpu-03", []string{"gpu-01", "gpu-02", "gpu-03"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var checked []string
					check := func(gotCtx context.Context, host string) error {
						checked = append(checked, host)
						if host == tc.cancelHost {
							cancel()
							return gotCtx.Err()
						}
						return nil
					}

					results, err := implementation.run(ctx, []string{"gpu-01", "gpu-02", "gpu-03"}, check)
					if !errors.Is(err, context.Canceled) {
						t.Errorf("batch error = %v, want context.Canceled", err)
					}
					if !slices.Equal(checked, tc.wantHosts) {
						t.Errorf("checked hosts = %v, want %v", checked, tc.wantHosts)
					}
					if len(results) != len(tc.wantHosts) {
						t.Fatalf("result count = %d, want %d", len(results), len(tc.wantHosts))
					}
					for i, host := range tc.wantHosts {
						if results[i].Host != host {
							t.Errorf("result %d host = %q, want %q", i, results[i].Host, host)
						}
						if i < len(results)-1 && results[i].Err != nil {
							t.Errorf("completed host %q error = %v, want nil", host, results[i].Err)
						}
					}
					if !errors.Is(results[len(results)-1].Err, context.Canceled) {
						t.Errorf("interrupted host error = %v, want context.Canceled", results[len(results)-1].Err)
					}
				})
			}
		})
	}
}

func TestCheckHostsKeepsHostDeadlineLocal(t *testing.T) {
	for _, implementation := range checkerImplementations {
		t.Run(implementation.name, func(t *testing.T) {
			ctx := context.Background()
			results, err := implementation.run(ctx, []string{"a", "slow", "c"}, func(ctx context.Context, host string) error {
				if host == "slow" {
					hostCtx, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
					defer cancel()
					return hostCtx.Err()
				}
				return nil
			})
			if err != nil || len(results) != 3 {
				t.Fatalf("got %d results and error %v, want 3 and nil", len(results), err)
			}
			if results[0].Err != nil || !errors.Is(results[1].Err, context.DeadlineExceeded) || results[2].Err != nil {
				t.Errorf("unexpected per-host results: %+v", results)
			}
		})
	}
}
