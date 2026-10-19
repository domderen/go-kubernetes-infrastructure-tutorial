package lab

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// Catches creating a second job when the response/checkpoint containing the
// first job ID is lost. The retry supplies only the original request.
func TestEnsureImageJobRecoversLostResponse(t *testing.T) {
	provider, req := newImageRequest(t)
	created, err := provider.EnsureImageJob(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}

	// The caller loses created.ID before persisting any checkpoint. We keep it
	// only in the test harness to check which job the retry recovers.
	recovered, err := provider.EnsureImageJob(t.Context(), req)
	if err != nil || created.ID == "" || recovered != created || recovered.Request != req {
		t.Fatalf("created=%+v recovered=%+v error=%v; want the original job", created, recovered, err)
	}
	assertImageJobCount(t, provider, 1)
}

// Catches check-then-create races between overlapping activity attempts.
func TestEnsureImageJobConcurrentDuplicates(t *testing.T) {
	provider, req := newImageRequest(t)
	results := make([]ImageJob, 20)
	errs := make([]error, len(results))
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(len(results))
	for i := range results {
		go func() {
			defer workers.Done()
			<-start
			results[i], errs[i] = provider.EnsureImageJob(t.Context(), req)
		}()
	}
	close(start)
	workers.Wait()

	for i, job := range results {
		if errs[i] != nil || job.ID == "" || job != results[0] || job.Request != req {
			t.Fatalf("attempt %d returned %+v, %v; want one shared job", i, job, errs[i])
		}
	}
	assertImageJobCount(t, provider, 1)
}

// Catches silently accepting changed intent under an existing retry key.
func TestEnsureImageJobRejectsChangedRequest(t *testing.T) {
	for _, field := range []string{"host", "image"} {
		t.Run(field, func(t *testing.T) {
			provider, req := newImageRequest(t)
			original, err := provider.EnsureImageJob(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			changed := req
			if field == "host" {
				other, err := provider.EnsureHost(t.Context(), req.OwnerUID, 2)
				if err != nil {
					t.Fatal(err)
				}
				changed.HostID = other.ID
			} else {
				changed.ImageVersion = "os-v2"
			}
			if _, err := provider.EnsureImageJob(t.Context(), changed); !errors.Is(err, ErrImageRequestConflict) {
				t.Fatalf("changed request error=%v; want conflict", err)
			}
			recovered, err := provider.EnsureImageJob(t.Context(), req)
			if err != nil || recovered != original {
				t.Fatalf("conflict changed the original job: %+v, %v", recovered, err)
			}
			assertImageJobCount(t, provider, 1)
		})
	}
}

// Catches conflating different operations or different cluster incarnations.
func TestEnsureImageJobSeparatesOwnerAndOperationKeys(t *testing.T) {
	for _, field := range []string{"owner", "operation"} {
		t.Run(field, func(t *testing.T) {
			provider, req := newImageRequest(t)
			first, err := provider.EnsureImageJob(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			other := req
			if field == "owner" {
				host, err := provider.EnsureHost(t.Context(), "uid-B", 1)
				if err != nil {
					t.Fatal(err)
				}
				other.OwnerUID, other.HostID = "uid-B", host.ID
			} else {
				other.OperationID = "image-revision-2"
				other.ImageVersion = "os-v2"
			}
			second, err := provider.EnsureImageJob(t.Context(), other)
			if err != nil || second.ID == "" || second.ID == first.ID || second.Request != other {
				t.Fatalf("distinct request conflated: first=%+v second=%+v error=%v", first, second, err)
			}
			assertImageJobCount(t, provider, 2)
		})
	}
}

// Catches invalid, unowned, or canceled requests recording provider jobs.
func TestEnsureImageJobRejectsInvalidAndCanceledRequests(t *testing.T) {
	for _, tc := range []struct {
		name string
		want error
	}{
		{"empty owner", ErrInvalidImageRequest},
		{"empty operation", ErrInvalidImageRequest},
		{"empty host", ErrInvalidImageRequest},
		{"empty image", ErrInvalidImageRequest},
		{"unknown host", ErrHostNotOwned},
		{"other owner", ErrHostNotOwned},
		{"canceled", context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider, req := newImageRequest(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch tc.name {
			case "empty owner":
				req.OwnerUID = ""
			case "empty operation":
				req.OperationID = ""
			case "empty host":
				req.HostID = ""
			case "empty image":
				req.ImageVersion = ""
			case "unknown host":
				req.HostID = "not-allocated"
			case "other owner":
				req.OwnerUID = "uid-B"
			case "canceled":
				cancel()
			}
			if _, err := provider.EnsureImageJob(ctx, req); !errors.Is(err, tc.want) {
				t.Fatalf("error=%v; want %v", err, tc.want)
			}
			assertImageJobCount(t, provider, 0)
		})
	}
}

// Catches late retries or new imaging requests being accepted after cleanup.
func TestEnsureImageJobRejectsReleasedOwner(t *testing.T) {
	provider, req := newImageRequest(t)
	if _, err := provider.EnsureImageJob(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if err := provider.ReleaseHosts(t.Context(), req.OwnerUID); err != nil {
		t.Fatal(err)
	}
	for _, operationID := range []string{req.OperationID, "late-operation"} {
		late := req
		late.OperationID = operationID
		if _, err := provider.EnsureImageJob(t.Context(), late); !errors.Is(err, ErrOwnerReleased) {
			t.Fatalf("late operation %q error=%v; want released owner", operationID, err)
		}
	}
	// Job records remain as history; no real imaging operation runs in this lab.
	assertImageJobCount(t, provider, 1)
}

func newImageRequest(t *testing.T) (*SimulatedAllocator, ImageRequest) {
	t.Helper()
	provider := &SimulatedAllocator{}
	host, err := provider.EnsureHost(t.Context(), "uid-A", 1)
	if err != nil {
		t.Fatal(err)
	}
	return provider, ImageRequest{OwnerUID: "uid-A", OperationID: "image-revision-1", HostID: host.ID, ImageVersion: "os-v1"}
}

func assertImageJobCount(t *testing.T, provider *SimulatedAllocator, want int) {
	t.Helper()
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if got := len(provider.imageJobs); got != want {
		t.Fatalf("provider job count=%d; want %d", got, want)
	}
}

// Catches losing a completion update, replacing a job on retry, or treating a
// newly accepted job as already finished.
func TestImageJobCompletionIsObservedByRetry(t *testing.T) {
	provider, req := newImageRequest(t)
	job, err := provider.EnsureImageJob(t.Context(), req)
	if err != nil || job.Done {
		t.Fatalf("new job=%+v, %v; want pending", job, err)
	}
	pending, err := provider.GetImageJob(t.Context(), req.OwnerUID, job.ID)
	if err != nil || pending != job {
		t.Fatalf("pending observation=%+v, %v; want %+v", pending, err, job)
	}
	for range 2 {
		completed, err := provider.CompleteImageJob(t.Context(), req.OwnerUID, job.ID)
		if err != nil || !completed.Done || completed.ID != job.ID || completed.Request != req {
			t.Fatalf("completion=%+v, %v; want same job marked done", completed, err)
		}
	}
	observed, err := provider.GetImageJob(t.Context(), req.OwnerUID, job.ID)
	if err != nil || !observed.Done || observed.ID != job.ID {
		t.Fatalf("completed observation=%+v, %v", observed, err)
	}
	retried, err := provider.EnsureImageJob(t.Context(), req)
	if err != nil || retried != observed {
		t.Fatalf("retry lost completion: %+v, %v; want %+v", retried, err, observed)
	}
	assertImageJobCount(t, provider, 1)
}

// Catches observing another owner's job or completing it through invalid or
// canceled calls. Both reads and completion events must preserve identity.
func TestImageJobStatusRejectsUnownedAndCanceledCalls(t *testing.T) {
	for _, tc := range []struct {
		name string
		want error
	}{
		{"other owner", ErrImageJobNotFound},
		{"unknown job", ErrImageJobNotFound},
		{"canceled", context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider, req := newImageRequest(t)
			job, err := provider.EnsureImageJob(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ownerUID, jobID := req.OwnerUID, job.ID
			switch tc.name {
			case "other owner":
				ownerUID = "uid-B"
			case "unknown job":
				jobID = "missing-job"
			case "canceled":
				cancel()
			}
			if _, err := provider.GetImageJob(ctx, ownerUID, jobID); !errors.Is(err, tc.want) {
				t.Fatalf("read error=%v; want %v", err, tc.want)
			}
			if _, err := provider.CompleteImageJob(ctx, ownerUID, jobID); !errors.Is(err, tc.want) {
				t.Fatalf("completion error=%v; want %v", err, tc.want)
			}
			observed, err := provider.GetImageJob(t.Context(), req.OwnerUID, job.ID)
			if err != nil || observed != job {
				t.Fatalf("rejected call changed the job: %+v, %v", observed, err)
			}
		})
	}
}

// Catches equating closed ownership with cancellation of already accepted
// provider work. Activities stop polling, but a late completion remains possible.
func TestImageJobCanCompleteAfterOwnerCleanup(t *testing.T) {
	provider, req := newImageRequest(t)
	job, err := provider.EnsureImageJob(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.ReleaseHosts(t.Context(), req.OwnerUID); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.GetImageJob(t.Context(), req.OwnerUID, job.ID); !errors.Is(err, ErrOwnerReleased) {
		t.Fatalf("poll after cleanup error=%v; want released owner", err)
	}
	completed, err := provider.CompleteImageJob(t.Context(), req.OwnerUID, job.ID)
	if err != nil || !completed.Done || completed.ID != job.ID {
		t.Fatalf("late provider completion=%+v, %v", completed, err)
	}
	assertImageJobCount(t, provider, 1)
}
