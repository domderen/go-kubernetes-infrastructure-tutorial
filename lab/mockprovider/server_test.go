package mockprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
)

func startProvider(t *testing.T, path string, delay time.Duration) (*httptest.Server, *Client) {
	t.Helper()
	provider, err := New(path, delay)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(provider.Handler())
	t.Cleanup(server.Close)
	return server, NewClient(server.URL)
}

func inventory(t *testing.T, serverURL string) Inventory {
	t.Helper()
	response, err := http.Get(serverURL + "/v1/inventory")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("inventory status = %d", response.StatusCode)
	}
	var result Inventory
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func postFaults(t *testing.T, serverURL string, faults map[string]any) {
	t.Helper()
	body, err := json.Marshal(faults)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(serverURL+"/v1/faults", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("fault status = %d, body = %s", response.StatusCode, data)
	}
}

// Losing the in-memory index must not lose an accepted provider operation.
func TestReopenPreservesAllocationAndImageIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "provider.json")
	firstServer, first := startProvider(t, path, 40*time.Millisecond)
	host, err := first.EnsureHost(ctx, "owner-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	request := lab.ImageRequest{OwnerUID: "owner-a", OperationID: "intent/slot-1", HostID: host.ID, ImageVersion: "os-v1"}
	job, err := first.EnsureImageJob(ctx, request)
	if err != nil || job.Done {
		t.Fatalf("initial job = %+v, err = %v", job, err)
	}
	firstServer.Close()
	time.Sleep(50 * time.Millisecond)
	secondServer, second := startProvider(t, path, time.Hour)
	recoveredHost, err := second.EnsureHost(ctx, "owner-a", 1)
	if err != nil || recoveredHost != host {
		t.Fatalf("host after reopen = %+v, %v; want %+v", recoveredHost, err, host)
	}
	recoveredJob, err := second.EnsureImageJob(ctx, request)
	if err != nil || recoveredJob.ID != job.ID || !recoveredJob.Done {
		t.Fatalf("job after reopen = %+v, %v; want original completed ID %s", recoveredJob, err, job.ID)
	}
	got := inventory(t, secondServer.URL)
	if len(got.Hosts) != 1 || len(got.Jobs) != 1 || got.Counters.HostsCreated != 1 || got.Counters.ImagesCreated != 1 {
		t.Fatalf("reopen duplicated resources: %+v", got)
	}
}

// A failed HTTP exchange after commit must be recoverable without its returned ID.
func TestLostResponsesRecoverCommittedResourcesOnce(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "provider.json")
	server, client := startProvider(t, path, 0)
	postFaults(t, server.URL, map[string]any{"ownerUID": "owner-a", "loseAllocationResponseOnce": true, "loseImageResponseOnce": true})
	if _, err := client.EnsureHost(ctx, "owner-a", 1); err == nil {
		t.Fatal("lost allocation response unexpectedly succeeded")
	}
	server.Close()
	server, client = startProvider(t, path, 0)
	host, err := client.EnsureHost(ctx, "owner-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	request := lab.ImageRequest{OwnerUID: "owner-a", OperationID: "image/1", HostID: host.ID, ImageVersion: "os-v1"}
	if _, err := client.EnsureImageJob(ctx, request); err == nil {
		t.Fatal("lost image response unexpectedly succeeded")
	}
	job, err := client.EnsureImageJob(ctx, request)
	if err != nil || job.ID == "" {
		t.Fatalf("recover image = %+v, %v", job, err)
	}
	got := inventory(t, server.URL)
	if len(got.Hosts) != 1 || len(got.Jobs) != 1 || got.Counters.HostsCreated != 1 || got.Counters.ImagesCreated != 1 || got.Counters.LostAllocationResponses != 1 || got.Counters.LostImageResponses != 1 {
		t.Fatalf("incorrect committed inventory: %+v", got)
	}
}

// Parallel retries must share the same atomic key lookup and create boundary.
func TestConcurrentDuplicatesCreateOneHostAndImage(t *testing.T) {
	server, client := startProvider(t, filepath.Join(t.TempDir(), "state.json"), 0)
	const duplicates = 20
	errs := make(chan error, duplicates)
	var workers sync.WaitGroup
	for range duplicates {
		workers.Go(func() {
			host, err := client.EnsureHost(context.Background(), "owner-a", 1)
			if err == nil {
				_, err = client.EnsureImageJob(context.Background(), lab.ImageRequest{OwnerUID: "owner-a", OperationID: "image/1", HostID: host.ID, ImageVersion: "os-v1"})
			}
			errs <- err
		})
	}
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got := inventory(t, server.URL)
	if len(got.Hosts) != 1 || len(got.Jobs) != 1 || got.Counters.HostsCreated != 1 || got.Counters.ImagesCreated != 1 {
		t.Fatalf("concurrent retries created duplicates: %+v", got)
	}
}

// Cleanup must fence delayed writes and retain accepted external work as history.
func TestReleaseFencesOwnerAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.json")
	server, client := startProvider(t, path, 0)
	host, err := client.EnsureHost(ctx, "owner-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	request := lab.ImageRequest{OwnerUID: "owner-a", OperationID: "image/1", HostID: host.ID, ImageVersion: "os-v1"}
	job, err := client.EnsureImageJob(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	other, err := client.EnsureHost(ctx, "owner-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ReleaseHosts(ctx, "owner-a"); err != nil {
		t.Fatal(err)
	}
	server.Close()
	server, client = startProvider(t, path, 0)
	if err := client.ReleaseHosts(ctx, "owner-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EnsureHost(ctx, "owner-a", 2); !errors.Is(err, lab.ErrOwnerReleased) {
		t.Fatalf("late allocation error = %v", err)
	}
	if _, err := client.EnsureImageJob(ctx, request); !errors.Is(err, lab.ErrOwnerReleased) {
		t.Fatalf("late image error = %v", err)
	}
	if _, err := client.GetImageJob(ctx, "owner-a", job.ID); !errors.Is(err, lab.ErrOwnerReleased) {
		t.Fatalf("late poll error = %v", err)
	}
	operation := lab.HostOperation{OwnerUID: "owner-a", OperationID: "join/1", HostID: host.ID, GPUClass: "mock-a100"}
	if err := client.JoinCluster(ctx, operation); !errors.Is(err, lab.ErrOwnerReleased) {
		t.Fatalf("late join error = %v", err)
	}
	if err := client.ValidateGPU(ctx, operation); !errors.Is(err, lab.ErrOwnerReleased) {
		t.Fatalf("late validate error = %v", err)
	}
	got := inventory(t, server.URL)
	if len(got.Hosts) != 1 || got.Hosts[0].ID != other.ID || len(got.Jobs) != 1 || len(got.ClosedOwners) != 1 || got.ClosedOwners[0] != "owner-a" || got.Counters.Releases != 1 {
		t.Fatalf("cleanup lost history, touched another owner, or duplicated release: %+v", got)
	}
}

// The provider, rather than the caller, enforces ordered lifecycle and immutable keys.
func TestLifecycleRequiresImageAndJoinAndPreservesKeys(t *testing.T) {
	ctx := context.Background()
	server, client := startProvider(t, filepath.Join(t.TempDir(), "state.json"), time.Hour)
	postFaults(t, server.URL, map[string]any{"ownerUID": "owner-a", "imageDelayMillis": 0})
	host, err := client.EnsureHost(ctx, "owner-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	op := lab.HostOperation{OwnerUID: "owner-a", OperationID: "join/1", HostID: host.ID, GPUClass: "mock-a100"}
	if err := client.JoinCluster(ctx, op); !errors.Is(err, ErrImageNotReady) {
		t.Fatalf("join without completed image = %v", err)
	}
	if err := client.ValidateGPU(ctx, op); !errors.Is(err, ErrHostNotJoined) {
		t.Fatalf("validate before join = %v", err)
	}
	request := lab.ImageRequest{OwnerUID: "owner-a", OperationID: "image/1", HostID: host.ID, ImageVersion: "os-v1"}
	if _, err := client.EnsureImageJob(ctx, request); err != nil {
		t.Fatal(err)
	}
	changedImage := request
	changedImage.ImageVersion = "os-v2"
	if _, err := client.EnsureImageJob(ctx, changedImage); !errors.Is(err, lab.ErrImageRequestConflict) {
		t.Fatalf("changed image request = %v", err)
	}
	for range 2 {
		if err := client.JoinCluster(ctx, op); err != nil {
			t.Fatal(err)
		}
	}
	changedJoin := op
	changedJoin.GPUClass = "mock-h100"
	if err := client.JoinCluster(ctx, changedJoin); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("changed join request = %v", err)
	}
	op.OperationID = "validate/1"
	invalid := op
	invalid.GPUClass = "real-gpu"
	if err := client.ValidateGPU(ctx, invalid); !errors.Is(err, lab.ErrUnsupportedGPUClass) {
		t.Fatalf("unsupported GPU = %v", err)
	}
	postFaults(t, server.URL, map[string]any{"ownerUID": "owner-a", "failValidation": true})
	if err := client.ValidateGPU(ctx, op); !errors.Is(err, lab.ErrValidationFailed) {
		t.Fatalf("terminal validation fault = %v", err)
	}
	postFaults(t, server.URL, map[string]any{"ownerUID": "owner-a", "failValidation": false})
	for range 2 {
		if err := client.ValidateGPU(ctx, op); err != nil {
			t.Fatal(err)
		}
	}
	got := inventory(t, server.URL)
	if len(got.Hosts) != 1 || !got.Hosts[0].Joined || !got.Hosts[0].Validated || got.Counters.Joins != 1 || got.Counters.Validations != 1 {
		t.Fatalf("lifecycle side effects duplicated or absent: %+v", got)
	}
}

// Wire errors must retain classifications; caller cancellation must reach HTTP I/O.
func TestClientPreservesSentinelsAndCancellation(t *testing.T) {
	_, client := startProvider(t, filepath.Join(t.TempDir(), "state.json"), 0)
	ctx := context.Background()
	if _, err := client.EnsureHost(ctx, "", 0); !errors.Is(err, lab.ErrInvalidAllocation) {
		t.Fatalf("invalid allocation = %v", err)
	}
	if _, err := client.EnsureImageJob(ctx, lab.ImageRequest{}); !errors.Is(err, lab.ErrInvalidImageRequest) {
		t.Fatalf("invalid image = %v", err)
	}
	if _, err := client.GetImageJob(ctx, "owner-a", "missing"); !errors.Is(err, lab.ErrImageJobNotFound) {
		t.Fatalf("missing image = %v", err)
	}
	if _, err := client.EnsureImageJob(ctx, lab.ImageRequest{OwnerUID: "owner-a", OperationID: "image/1", HostID: "other", ImageVersion: "os-v1"}); !errors.Is(err, lab.ErrHostNotOwned) {
		t.Fatalf("unowned image = %v", err)
	}
	entered := make(chan struct{})
	blocking := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
	}))
	defer blocking.Close()
	blockingClient := NewClient(blocking.URL)
	cancelCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := blockingClient.ListHosts(cancelCtx, "owner-a")
		done <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled HTTP call = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not cancel in-flight HTTP call")
	}
}

// A failed durable commit must not publish speculative state in memory.
func TestFailedCommitDoesNotPublishAllocation(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "state.json")
	server, client := startProvider(t, path, 0)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EnsureHost(context.Background(), "owner-a", 1); err == nil {
		t.Fatal("allocation succeeded despite failed atomic replacement")
	}
	if got := inventory(t, server.URL); len(got.Hosts) != 0 || got.Counters.HostsCreated != 0 {
		t.Fatalf("failed commit leaked speculative allocation: %+v", got)
	}
}

// A concurrent ensure that finishes before release may create a host, but no
// interleaving may leave that host alive after the owner is closed.
func TestReleaseRacesAllocationWithoutOrphans(t *testing.T) {
	server, client := startProvider(t, filepath.Join(t.TempDir(), "state.json"), 0)
	ctx := context.Background()
	start := make(chan struct{})
	results := make(chan error, 40)
	var workers sync.WaitGroup
	for slot := 1; slot <= 40; slot++ {
		workers.Go(func() {
			<-start
			_, err := client.EnsureHost(ctx, "owner-a", slot)
			results <- err
		})
	}
	close(start)
	if err := client.ReleaseHosts(ctx, "owner-a"); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil && !errors.Is(err, lab.ErrOwnerReleased) {
			t.Fatal(err)
		}
	}
	if got := inventory(t, server.URL); len(got.Hosts) != 0 || len(got.ClosedOwners) != 1 {
		t.Fatalf("release left an orphan allocation: %+v", got)
	}
}

// Strict bounded JSON prevents a malformed control request from changing state.
func TestHTTPRejectsMalformedOversizedAndTrailingRequests(t *testing.T) {
	server, _ := startProvider(t, filepath.Join(t.TempDir(), "state.json"), 0)
	tests := []struct {
		name string
		body string
		want int
	}{
		{"malformed", "{", http.StatusBadRequest},
		{"unknown field", `{"ownerUID":"owner-a","slot":1,"typo":true}`, http.StatusBadRequest},
		{"two requests", `{"ownerUID":"owner-a","slot":1} {}`, http.StatusBadRequest},
		{"large value", `{"ownerUID":"` + strings.Repeat("x", 1<<20) + `","slot":1}`, http.StatusRequestEntityTooLarge},
		{"large trailing whitespace", `{"ownerUID":"owner-a","slot":1}` + strings.Repeat(" ", 1<<20), http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := http.Post(server.URL+"/v1/hosts/ensure", "application/json", strings.NewReader(test.body))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.want {
				t.Fatalf("HTTP status = %d, want %d", response.StatusCode, test.want)
			}
		})
	}
	if got := inventory(t, server.URL); len(got.Hosts) != 0 {
		t.Fatalf("invalid HTTP request allocated hosts: %+v", got)
	}
}

// Client limits apply even if a peer sends HTTP 200 and no Content-Length.
func TestClientRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, strings.Repeat(" ", (4<<20)+1))
	}))
	defer server.Close()
	if _, err := NewClient(server.URL).ListHosts(context.Background(), "owner-a"); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized response = %v", err)
	}
}
