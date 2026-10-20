# First Kubernetes adapter

This document describes the retained allocation-only teaching adapter. The
running system uses [provisioning_controller.go](provisioning_controller.go) and
[the Temporal adapter](provisioning_temporal.go); see [the live lab](../LIVE.md)
for its ownership, persistent workflow identity, status, and deletion behavior.

This increment wraps the existing `lab.ReconcileHosts` and
`SimulatedAllocator.ReleaseHosts` in a controller-runtime `Reconcile` method.
The tests install our CRD in a temporary API server and etcd. Most invoke the
adapter directly; a manager test runs the real watch/cache/queue loop and drives
reconciliation through API changes. The manager runs only for that test.

Read these files in order:

1. [The API types](../api/v1alpha1/gpucluster_types.go): `spec.nodeCount`
   requests 0–100 allocations. It is immutable in this increment; a later
   lesson will introduce a spec-change policy. This adapter uses nodeCount;
   the added image/GPU/concurrency fields belong to the integrated controller.
   Status reports allocated host IDs/slots and the generation
   observed, plus allocation conditions; allocation does not imply health or readiness.
2. [The controller](gpucluster_controller.go): begin with the deletion branch.
3. [The lifecycle tests](gpucluster_controller_test.go): examine the failed
   cleanup and lost finalizer-write scenarios.
4. [The manager test](gpucluster_manager_test.go): API create, status update,
   and delete drive the controller without direct Reconcile calls.

## What starts reconciliation

SetupWithManager registers a watch for GPUCluster objects. API changes enqueue
the object's namespace/name; a controller worker calls Reconcile with that key.
Reconcile then reads the object and queries the provider to discover whether
anything needs changing. The event handler does not first compare host counts.
Several events can produce one pass, and a pass can do no work when converged.

Provider inventory changes need an explicit signal or polling; our GPUCluster
watch does not receive provider events. After a successful allocation check,
Reconcile returns RequeueAfter using PollInterval (default 30 seconds for this
lab). This also happens when status is unchanged. Nonpositive configuration
uses the default. Lower intervals reduce detection delay and increase provider
requests; the interval is not a hard detection deadline when work is queued.
Non-nil errors instead use controller-runtime's retry backoff.

The initial finalizer write produces a watched API update, which queues the
next allocation pass. We use no generation-only event filter: finalizer,
deletion, and status updates matter too. The no-op status guard prevents our own
unchanged reports from generating an endless stream of writes/events.

Polling currently queries allocation inventory, not the separate health-check
fixtures. A host timeout does not by itself establish a lost allocation or
authorize replacement. There is no provider notification bridge yet.

## Deletion sequence

The API object stays present with a `deletionTimestamp` while finalizers remain.
Our controller calls `ReleaseHosts` for the object's UID. If cleanup fails, it
returns the error and keeps its key. After cleanup succeeds, it removes only
its own key and saves that change. Kubernetes can finish deleting the object
when no finalizers remain.

If the controller stops between cleanup and the API write, the next pass
repeats cleanup. Already-absent hosts count as success, allowing that pass to
remove the key. The provider also rejects delayed allocations for a released
UID. The finalizer is a metadata key; the controller executes the cleanup code.

## Allocation sequence

The first pass persists the finalizer; its watched update queues another pass. Allocation
only begins on a later pass. `ReconcileHosts` uses provider discovery and stable
`(UID, slot)` identities, then the adapter writes the observations through the
status subresource. Unchanged status is left alone. Metadata and status patches
include a resource-version precondition; a conflicting write returns an error
so a later pass can read current state.

## Allocation conditions

The `Allocated` condition reports whether the latest reconciliation could
confirm the requested allocations. A successful pass sets `True`, reason
`HostsAllocated`. A failed provider observation/allocation sets `Unknown`,
reason `ProviderError`, with the error in its message. This adapter does not
currently emit `False`; it does not have an asynchronous provisioning state.

During a provider timeout, keep the last successfully observed host count and
IDs. For example, these selected status fields mean "last reported three hosts;
the latest attempt could not confirm their current state":

```yaml
allocatedHosts: 3
conditions:
- type: Allocated
  status: "Unknown"
  reason: ProviderError
  message: "observe allocations: provider request timed out"
```

The top-level observedGeneration stays with the last successful capacity report;
each condition has its own observedGeneration identifying the spec for that
attempt. SetStatusCondition preserves other condition types and updates
lastTransitionTime when the status changes. That time records a transition,
not every observation. Repeating the same result produces no status write.

After recovery, a successful provider pass reports the existing allocations
and restores `Allocated=True`. Allocation confirmation does not establish host
health. The provider error is still returned for retry after reporting Unknown.
If saving the condition also fails, errors.Join preserves both causes and the
saved status remains the previous report.

## Verification

From `lab/`, with Homebrew Go on PATH on this Mac:

```sh
export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/go/bin:$PATH"
make test
```

If Apple's `make` selects an Xcode installation with a pending license prompt,
the already-installed Command Line Tools work with this per-command setting:

```sh
DEVELOPER_DIR=/Library/Developer/CommandLineTools make test
```

`make test` generates the checked-in deepcopy code and CRD, obtains native
envtest assets under ignored `lab/bin/`, and runs all tests with the
`integration` build tag and race detector. It pins controller-runtime
`v0.25.1`, Kubernetes libraries `v0.37.0`, API-server assets `1.37.0`,
controller-gen `v0.23.0`, and the setup-envtest revision in the Makefile.
`make unit` runs the checker/provider tests without the API-server suite.

The API tests cover protection before allocation, repeated convergence without
new status writes, a real resource-version conflict preserving another
finalizer, cleanup failure/retry and owner isolation, successful cleanup followed
by a failed finalizer write, lost status persistence, and API validation. They
also cover outage/Unknown/recovery without losing host identities, preservation
of another writer's condition, no write for unchanged failure, and both errors
when the provider and condition write fail together.

The manager test uses its cached client, watches a separate namespace to keep
provider state isolated from direct-call fixtures, and disables metrics/probe
listeners. It creates an object, observes automatic allocation, changes only
status and observes repair without new host IDs, then deletes through the API
and observes cleanup. Cancellation stops and joins the manager before the test
API server is stopped. Separate result-contract tests check default/configured
poll scheduling on both status-writing and converged passes.

The new-reconciler tests keep the same in-memory provider alive. They demonstrate
controller recovery logic, without demonstrating provider durability across a
process restart. The manager exists in the test harness; a standalone executable,
Kubernetes deployment, health integration, and controller-to-Temporal integration remain future increments.
No kind cluster is required.

Sources: [finalizers](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/),
[controller implementation](https://book.kubebuilder.io/reference/using-finalizers.html),
[envtest](https://book.kubebuilder.io/reference/envtest), and
[the selected dependency versions](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.1/go.mod).
Condition fields and update behavior follow the pinned Kubernetes
[Condition type](https://github.com/kubernetes/apimachinery/blob/v0.37.0/pkg/apis/meta/v1/types.go#L1507)
and [SetStatusCondition helper](https://github.com/kubernetes/apimachinery/blob/v0.37.0/pkg/api/meta/conditions.go).
Watch/queue and retry behavior follow the selected controller-runtime
[builder](https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.1/pkg/builder#TypedBuilder.For),
[reconciler](https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.1/pkg/reconcile#TypedReconciler),
and [manager](https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.1/pkg/manager#Manager).
