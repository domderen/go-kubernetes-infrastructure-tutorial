package provisioning

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Catches returning an accepted job before completion, never polling it, or
// omitting the job/host identity from the activity's heartbeat.
func TestEnsureImagePollsCompletionAndHeartbeats(t *testing.T) {
	provider, req := imageFixture(t)
	provider.completeAfter = 3
	env, activities := imageActivityEnvironment(provider, time.Millisecond)
	recorder := &heartbeatCallRecorder{}
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{recorder}})
	checkpoints := make(chan ImageCheckpoint, 8)
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		var checkpoint ImageCheckpoint
		if err := details.Get(&checkpoint); err != nil {
			t.Errorf("decode heartbeat: %v", err)
			return
		}
		select {
		case checkpoints <- checkpoint:
		default:
		}
	})
	encoded, err := env.ExecuteActivity(activities.EnsureImage, req)
	if err != nil {
		t.Fatal(err)
	}
	var result lab.ImageJob
	if err := encoded.Get(&result); err != nil {
		t.Fatal(err)
	}
	requests, created, polls := provider.snapshot()
	if !result.Done || result.ID == "" || result.ID != created.ID || result.Request != req || polls != 3 || len(requests) != 1 {
		t.Fatalf("result=%+v requests=%v polls=%d; want completion of the original job after 3 polls", result, requests, polls)
	}
	if recorder.calls.Load() < 2 {
		t.Fatalf("heartbeat calls=%d; want repeated reports while observing the job", recorder.calls.Load())
	}
	select {
	case checkpoint := <-checkpoints:
		if checkpoint.JobID != result.ID || checkpoint.HostID != req.HostID {
			t.Fatalf("heartbeat=%+v; want job=%q host=%q", checkpoint, result.ID, req.HostID)
		}
	default:
		t.Fatal("activity emitted no heartbeat")
	}
}

// Catches generating a fresh request key after a provider accepted imaging but
// lost its response. The real SDK retries; no checkpoint was emitted yet.
func TestEnsureImageWorkflowRetryWithoutCheckpoint(t *testing.T) {
	provider, req := imageFixture(t)
	provider.loseResponse, provider.completeAfter = true, 1
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(3 * time.Second)
	activities := &ImagingActivities{Provider: provider, PollInterval: time.Millisecond}
	env.RegisterActivityWithOptions(activities.EnsureImage, activity.RegisterOptions{Name: EnsureImageActivity})
	var attempts atomic.Int32
	env.SetOnActivityStartedListener(func(*activity.Info, context.Context, converter.EncodedValues) { attempts.Add(1) })
	env.ExecuteWorkflow(imageTestWorkflow, req)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result lab.ImageJob
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	requests, created, _ := provider.snapshot()
	if attempts.Load() != 2 || len(requests) != 2 || requests[0] != req || requests[1] != req {
		t.Fatalf("attempts=%d requests=%v; want 2 attempts with unchanged input", attempts.Load(), requests)
	}
	if !result.Done || result.ID == "" || result.ID != created.ID || result.Request != req {
		t.Fatalf("retry replaced the accepted job: created=%+v result=%+v", created, result)
	}
}

// Catches sleeping through cancellation and equating local cancellation with
// cancellation of the provider's accepted job. A one-hour poll wait must end.
func TestEnsureImageCancellationWhileWaiting(t *testing.T) {
	provider, req := imageFixture(t)
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	env, activities := imageActivityEnvironment(provider, time.Hour)
	env.SetWorkerOptions(worker.Options{BackgroundActivityContext: ctx})
	// Model the SDK cancellation cause; a plain worker-background cancellation
	// is deliberately distinguished from a logical activity cancellation.
	env.SetOnActivityHeartbeatListener(func(*activity.Info, converter.EncodedValues) {
		cancel(temporal.NewCanceledError())
	})
	_, err := env.ExecuteActivity(activities.EnsureImage, req)
	if !temporal.IsCanceledError(err) {
		t.Fatalf("error=%v; want canceled activity", err)
	}
	_, created, polls := provider.snapshot()
	if created.ID == "" || polls != 0 {
		t.Fatalf("created=%+v polls=%d; want cancellation before next poll", created, polls)
	}
	pending, err := provider.SimulatedAllocator.GetImageJob(t.Context(), req.OwnerUID, created.ID)
	if err != nil || pending.Done || pending.ID != created.ID {
		t.Fatalf("cancellation lost or completed provider work: %+v, %v", pending, err)
	}
	completed, err := provider.CompleteImageJob(t.Context(), req.OwnerUID, created.ID)
	if err != nil || !completed.Done {
		t.Fatalf("provider could not finish after local cancellation: %+v, %v", completed, err)
	}
}

// Catches replacing the activity context with Background in a provider call.
func TestEnsureImagePassesCancellationToProviderRead(t *testing.T) {
	provider, req := imageFixture(t)
	provider.blockRead, provider.entered = true, make(chan struct{})
	ctx, cancel := context.WithCancelCause(t.Context())
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-provider.entered:
			cancel(temporal.NewCanceledError())
		case <-ctx.Done():
		}
	}()
	defer func() { cancel(nil); <-joined }()
	env, activities := imageActivityEnvironment(provider, time.Millisecond)
	env.SetWorkerOptions(worker.Options{BackgroundActivityContext: ctx})
	_, err := env.ExecuteActivity(activities.EnsureImage, req)
	if !temporal.IsCanceledError(err) {
		t.Fatalf("error=%v; want canceled provider read", err)
	}
	_, _, polls := provider.snapshot()
	if polls != 1 {
		t.Fatalf("provider reads=%d; want one canceled read", polls)
	}
}

// Catches ignoring observation errors or spinning inside the activity instead
// of returning a retryable error to the workflow's retry policy.
func TestEnsureImageReturnsProviderReadError(t *testing.T) {
	provider, req := imageFixture(t)
	provider.readErr = errors.New("status unavailable")
	env, activities := imageActivityEnvironment(provider, time.Millisecond)
	_, err := env.ExecuteActivity(activities.EnsureImage, req)
	var appErr *temporal.ApplicationError
	if err == nil || !strings.Contains(err.Error(), "status unavailable") || !errors.As(err, &appErr) || appErr.NonRetryable() {
		t.Fatalf("error=%v; want retryable provider observation error", err)
	}
	_, _, polls := provider.snapshot()
	if polls != 1 {
		t.Fatalf("provider reads=%d; want one failed read", polls)
	}
}

// Catches treating permanent request/ownership failures as transient retries.
func TestEnsureImageClassifiesPermanentRequestErrors(t *testing.T) {
	for _, name := range []string{"invalid", "conflict", "unowned", "released"} {
		t.Run(name, func(t *testing.T) {
			provider, req := imageFixture(t)
			switch name {
			case "invalid":
				req.ImageVersion = ""
			case "conflict":
				if _, err := provider.SimulatedAllocator.EnsureImageJob(t.Context(), req); err != nil {
					t.Fatal(err)
				}
				req.ImageVersion = "os-v3"
			case "unowned":
				req.OwnerUID = "cluster-B"
			case "released":
				if err := provider.ReleaseHosts(t.Context(), req.OwnerUID); err != nil {
					t.Fatal(err)
				}
			}
			env, activities := imageActivityEnvironment(provider, time.Millisecond)
			_, err := env.ExecuteActivity(activities.EnsureImage, req)
			assertNonRetryable(t, err)
			_, _, polls := provider.snapshot()
			if polls != 0 {
				t.Fatalf("permanent request error caused %d polls", polls)
			}
		})
	}
}

// Catches continuing to wait after cleanup closes a UID during imaging.
func TestEnsureImageStopsWhenOwnerIsReleasedDuringPolling(t *testing.T) {
	provider, req := imageFixture(t)
	env, activities := imageActivityEnvironment(provider, time.Millisecond)
	env.SetOnActivityHeartbeatListener(func(*activity.Info, converter.EncodedValues) {
		if err := provider.ReleaseHosts(t.Context(), req.OwnerUID); err != nil {
			t.Errorf("release owner: %v", err)
		}
	})
	_, err := env.ExecuteActivity(activities.EnsureImage, req)
	assertNonRetryable(t, err)
	if !strings.Contains(err.Error(), lab.ErrOwnerReleased.Error()) {
		t.Fatalf("error=%v; want released-owner cause", err)
	}
}

// This test-only workflow exercises retry policy without adding imaging to
// the production reservation-only ProvisionCluster workflow yet.
func imageTestWorkflow(ctx workflow.Context, req lab.ImageRequest) (lab.ImageJob, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second, ScheduleToCloseTimeout: 30 * time.Second,
		HeartbeatTimeout: time.Second,
		RetryPolicy:      &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, MaximumAttempts: 3},
	})
	var result lab.ImageJob
	err := workflow.ExecuteActivity(ctx, EnsureImageActivity, req).Get(ctx, &result)
	return result, err
}

func imageActivityEnvironment(provider ImageProvider, interval time.Duration) (*testsuite.TestActivityEnvironment, *ImagingActivities) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.SetTestTimeout(2 * time.Second)
	activities := &ImagingActivities{Provider: provider, PollInterval: interval}
	env.RegisterActivity(activities.EnsureImage)
	return env, activities
}

func imageFixture(t *testing.T) (*imageProbe, lab.ImageRequest) {
	t.Helper()
	provider := &imageProbe{SimulatedAllocator: &lab.SimulatedAllocator{}}
	host, err := provider.EnsureHost(t.Context(), "cluster-A", 1)
	if err != nil {
		t.Fatal(err)
	}
	return provider, lab.ImageRequest{OwnerUID: "cluster-A", OperationID: "image-2", HostID: host.ID, ImageVersion: "os-v2"}
}

// Real provider state survives retries. Only responses and external completion
// timing are controlled; no stored job or idempotency logic is mocked away.
type imageProbe struct {
	*lab.SimulatedAllocator
	mu            sync.Mutex
	requests      []lab.ImageRequest
	firstJob      lab.ImageJob
	polls         int
	loseResponse  bool
	completeAfter int
	readErr       error
	blockRead     bool
	entered       chan struct{}
	enteredOnce   sync.Once
}

func (p *imageProbe) EnsureImageJob(ctx context.Context, req lab.ImageRequest) (lab.ImageJob, error) {
	job, err := p.SimulatedAllocator.EnsureImageJob(ctx, req)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, req)
	if err == nil && p.firstJob.ID == "" {
		p.firstJob = job
	}
	if err == nil && p.loseResponse {
		p.loseResponse = false
		return lab.ImageJob{}, errors.New("image creation response lost")
	}
	return job, err
}

func (p *imageProbe) GetImageJob(ctx context.Context, ownerUID, jobID string) (lab.ImageJob, error) {
	p.mu.Lock()
	p.polls++
	polls, completeAfter, readErr, blockRead := p.polls, p.completeAfter, p.readErr, p.blockRead
	p.mu.Unlock()
	if blockRead {
		p.enteredOnce.Do(func() { close(p.entered) })
		<-ctx.Done()
		return lab.ImageJob{}, ctx.Err()
	}
	if readErr != nil {
		return lab.ImageJob{}, readErr
	}
	if completeAfter > 0 && polls == completeAfter {
		if _, err := p.CompleteImageJob(ctx, ownerUID, jobID); err != nil {
			return lab.ImageJob{}, err
		}
	}
	return p.SimulatedAllocator.GetImageJob(ctx, ownerUID, jobID)
}

func (p *imageProbe) snapshot() ([]lab.ImageRequest, lab.ImageJob, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]lab.ImageRequest(nil), p.requests...), p.firstJob, p.polls
}

// The SDK listener batches deliveries. Intercept calls before batching to catch
// a regression that reports once at startup but never again while polling.
type heartbeatCallRecorder struct {
	interceptor.WorkerInterceptorBase
	calls atomic.Int32
}

func (r *heartbeatCallRecorder) InterceptActivity(_ context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &heartbeatInbound{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, recorder: r}
}

type heartbeatInbound struct {
	interceptor.ActivityInboundInterceptorBase
	recorder *heartbeatCallRecorder
}

func (i *heartbeatInbound) Init(next interceptor.ActivityOutboundInterceptor) error {
	return i.Next.Init(&heartbeatOutbound{ActivityOutboundInterceptorBase: interceptor.ActivityOutboundInterceptorBase{Next: next}, recorder: i.recorder})
}

type heartbeatOutbound struct {
	interceptor.ActivityOutboundInterceptorBase
	recorder *heartbeatCallRecorder
}

func (o *heartbeatOutbound) RecordHeartbeat(ctx context.Context, details ...any) {
	o.recorder.calls.Add(1)
	o.Next.RecordHeartbeat(ctx, details...)
}
