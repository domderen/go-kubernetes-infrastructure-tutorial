# Temporal provisioning lifecycle

`ProvisionCluster` runs `ReserveHosts`, then a bounded number of host pipelines:

```text
EnsureImage → JoinCluster → ValidateGPU
```

It returns the reserved allocations after every pipeline succeeds. Joining and
GPU validation are simulated provider actions; this workflow does not install
drivers or create Kubernetes Nodes. The executable controller projects the
workflow outcome into GPUCluster status. The allocation-only controller remains
available as an earlier comparison.

Read [workflow.go](workflow.go), [activities.go](activities.go),
[imaging.go](imaging.go), and [lifecycle.go](lifecycle.go). Tests in
[workflow_test.go](workflow_test.go) preserve the original reservation/imaging
failure cases; [lifecycle_test.go](lifecycle_test.go) covers the full lifecycle.

## Identity and ownership

`Request` carries `ClusterUID`, `NodeCount`, `OperationID`, `ImageVersion`,
`GPUClass`, and `MaxParallel`. Missing operation/image intent or parallelism
outside 1–10 fails before reservation. Zero parallelism defaults to one for
earlier callers; the integrated Kubernetes API defaults to two. Empty GPUClass
defaults to `mock-a100`; the mock provider also supports `mock-h100`.
`ReserveHosts` validates UID and node count 0–100 at its activity boundary.

ClusterUID owns allocations throughout the flow. It is never replaced by a
workflow run ID or activity attempt. Reservation uses the existing
`ReconcileHosts`: discover owned hosts, then atomically ensure missing
`(ClusterUID, slot)` allocations. Stable ensures protect against duplicates even
when a response is lost and discovery is stale.

For the caller's `OperationID = revision-1`, slot 1 receives these operation IDs:

| Step | Stable operation ID |
| --- | --- |
| Image | `revision-1/image/1` |
| Join | `revision-1/join/1` |
| Validate | `revision-1/validate/1` |

Each step keeps the same owner, host, class/image parameters, and key on retry.
The provider owns atomic request binding and deduplication. A new intent needs a
new OperationID. Kubernetes generation/workflow identity is selected by the
controller, outside this workflow.

Workflow coroutines claim successive indices from the recorded reservation
slice. SDK scheduling is deterministic; native goroutines, timers, mutexes,
network calls, and provider state do not enter workflow code. Each worker waits
for one host's complete pipeline before claiming another. `MaxParallel` bounds
host pipelines within one workflow, including their retry waits; it is not a
global provider/worker concurrency limit. Returned inventory retains the
reservation's slice order, regardless of activity completion order.

## Activity boundaries and retries

Activities use ordinary `context.Context` for provider I/O. Consumer interfaces
live beside their activities: `ImageProvider` supplies image jobs and
`LifecycleProvider` supplies JoinCluster, ValidateGPU, and owner-scoped
ReleaseHosts. `Activities` continues to consume `lab.HostAllocator`.

EnsureImage calls `EnsureImageJob` on every attempt using the same request. It
records job/host heartbeat details after each provider observation, then polls
until Done with a cancelable timer. PollInterval defaults to 1s. Heartbeats are
observations, not a separate goroutine keeping a blocked provider call alive.
Provider idempotency recovers the job even when no checkpoint was delivered.

Durably recorded activity completions are reused during replay. An unrecorded
completion can cause another activity attempt: Temporal coordinates retries;
the provider's stable key prevents duplicate side effects. Replaying the
workflow is separate from executing a new activity observation.

The following teaching budgets apply separately to each activity execution:

| Activities | StartToClose | ScheduleToClose | HeartbeatTimeout |
| --- | --- | --- | --- |
| ReserveHosts, JoinCluster, ValidateGPU, ReleaseHosts | 5s | 30s | none |
| EnsureImage | 30m | 1h | 30s |

All use at most three attempts, with 100ms initial retry delay, coefficient 2,
and 1s maximum delay. All set `WaitForCancellation: true`. Short provider calls
must honor their activity context; remote cancellation delivery for a
non-heartbeating short activity may wait for its return or timeout.

Invalid reservation inputs, released owners, unsupported scale-down, invalid or
conflicting image requests, wrong ownership, unsupported GPU class, and terminal
validation failure are non-retryable. Other provider errors use the bounded
retry policy. A permanent error in one pipeline fails the cluster operation.

## Failure, cancellation, and explicit compensation

The first pipeline failure stops dispatch of later hosts and cancels sibling
work. Workflow workers are joined before compensation. With
WaitForCancellation enabled, real activity futures wait for acknowledgment or
timeout; this does not forcibly stop external work that ignores cancellation.
The original failure remains the workflow error rather than being replaced by
a sibling's cancellation.

After a valid reservation attempt, any failure or workflow cancellation
explicitly runs `ReleaseHosts(ClusterUID)`. This also covers partially committed
reservation attempts whose responses failed. Invalid requests that cannot reach
the provider do not trigger release. A disconnected workflow context lets
cleanup run after cancellation, using the bounded short-activity policy above.
If cleanup also fails, the returned error retains the original cause and the
cleanup failure message. A later controller deletion can retry cleanup.

Release must be idempotent and close the owner UID before removing allocations.
Controller deletion independently fences/releases the owner before requesting
workflow cancellation. Delayed activity calls then fail with ErrOwnerReleased.
Accepted image-job history remains uncertain external work: release does not
undo or cancel an already accepted imaging operation. No database rollback or
physical GPU cleanup is claimed.

## Worker registration

Register `ProvisionCluster` and every activity using its exported name constant:

```go
w.RegisterWorkflow(provisioning.ProvisionCluster)
allocation := &provisioning.Activities{Provider: provider}
imaging := &provisioning.ImagingActivities{Provider: provider}
lifecycle := &provisioning.LifecycleActivities{Provider: provider}
w.RegisterActivityWithOptions(allocation.ReserveHosts,
    activity.RegisterOptions{Name: provisioning.ReserveHostsActivity})
w.RegisterActivityWithOptions(imaging.EnsureImage,
    activity.RegisterOptions{Name: provisioning.EnsureImageActivity})
w.RegisterActivityWithOptions(lifecycle.JoinCluster,
    activity.RegisterOptions{Name: provisioning.JoinClusterActivity})
w.RegisterActivityWithOptions(lifecycle.ValidateGPU,
    activity.RegisterOptions{Name: provisioning.ValidateGPUActivity})
w.RegisterActivityWithOptions(lifecycle.ReleaseHosts,
    activity.RegisterOptions{Name: provisioning.ReleaseHostsActivity})
```

The names are `ReserveHosts`, `EnsureImage`, `JoinCluster`, `ValidateGPU`, and
`ReleaseHosts`. See [cmd/worker](../cmd/worker/main.go) for executable wiring.

## Verification and limits

From `lab/`, run `go test -race -count=1 -timeout=30s ./provisioning`.
`make unit` includes these tests; `make test` adds Kubernetes/envtest integration.
Use Homebrew Go and the environment settings in the workspace notes on this Mac.

Tests use real workflows/activities and the in-memory allocator, with controlled
provider completion/failure timing. They cover stable allocation/image/join
retry keys, bounded complete host pipelines, lifecycle sequencing, terminal
errors, partial reservation compensation, bounded cleanup failure, and
cancellation after image jobs have started. Existing activity tests cover
polling, heartbeat observations, provider context propagation, and image history
after release.

The [SDK v1.50.0 test environment](https://github.com/temporalio/sdk-go/blob/v1.50.0/internal/internal_workflow_testsuite.go#L1257)
immediately resolves canceled activity futures
without modeling real WaitForCancellation acknowledgment ordering. Cancellation
tests therefore stop/join in-process fixtures separately; live server/history
verification is required for actual cancellation delivery, worker restart,
provider durability, heartbeat batching, and replay across process restart.

Sources: [workflow cancellation](https://docs.temporal.io/develop/go/workflows/cancellation),
[activity idempotency](https://docs.temporal.io/activity-definition#idempotency),
[workflow basics](https://docs.temporal.io/develop/go/workflows/basics),
[activity timeouts](https://docs.temporal.io/develop/go/activities/timeouts), and
[SDK testing](https://docs.temporal.io/develop/go/best-practices/testing-suite).
