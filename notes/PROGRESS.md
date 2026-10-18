# Learning progress

Last updated: 18 October 2026 (Europe/Warsaw), end of session.

## Resume here

The learner stopped for the day and plans to continue from a fresh session on 19 October. Start with the unanswered question below; do not restart the Go diagnostics or environment setup.

> Provider cleanup succeeds, but the controller crashes before removing the GPUCluster finalizer. What should the next reconciliation do?

The learner has not answered this question. After discussing recovery, implement the first Kubernetes GPUCluster resource/controller adapter around the existing allocation and cleanup logic. Select and pin compatible dependencies and matching native envtest assets at that point. Keep setup bounded. No Kubernetes scaffold or API resource has been installed yet.

Preserve these invariants: persist the finalizer before external allocation; retain it on cleanup failure; remove it only after confirmed cleanup; use cluster UID for ownership and stable (UID, slot) keys for allocation retries. Actual finalizer writes/removals remain to implement.

## Teaching preferences and baseline

- Goal: learn Go, Kubernetes, Temporal, and infrastructure by building one cumulative simulated GPU control plane.
- Assistant writes small, reviewable increments and meaningful tests. Learner reads code, reviews design, predicts system behavior, and diagnoses failures; no syntax-copying or hand-typing exercises.
- Learner flagged excessive isolated Go gotchas. Center lessons on infrastructure behavior and explain language details when the project needs them. Remaining Module 1 topics do not block Module 2.
- Go: reports prior channel/select use and mostly having seen context.Context; demonstrated concepts are recorded below.
- Kubernetes: mainly workload deployment, some CRD/controller use, limited experience writing controllers; operator terminology was explained, not independently assessed.
- Temporal experience and available study time remain unassessed. No personal background should be inferred.
- Keep both sequential and concurrent checker implementations for comparison.

## Curriculum status

| Module | Status | Next deliverable |
| --- | --- | --- |
| 1: Go concurrency, cancellation, errors, diagnostics | In progress; checker and simulations implemented | Revisit remaining language/diagnostic topics when needed by the project. |
| 2: Kubernetes reconciliation and lifecycle | In progress; allocation business logic implemented | Actual GPUCluster API/controller adapter, lifecycle and API-server tests. |
| 3: Temporal durability and retries | Unassessed | ProvisionCluster workflow and activities after controller fundamentals. |
| 4: Workload capacity and diagnosis | Unassessed | Host lifecycle, capacity, scheduling, and failure diagnosis. |
| 5–6: Combined design and integration | Not started | Ownership, durable operation identity, integrated recovery. |
| 7: Technical walkthrough | Not started | Demonstrate behavior and explain failure/recovery/tradeoffs. |

No entire learning module is complete. Implementation/test results are not proof of learner understanding.

## Demonstrated understanding

Scores: 0 = cannot yet explain; 1 = correct concept; 2 = explains a failure case and a tradeoff. Unassessed is distinct from 0.

| Topic | Score | Observed evidence |
| --- | --- | --- |
| Bounded concurrency design | 2 | Explained resource exhaustion, slot occupancy, actual in-flight limits, collector ownership/shutdown, and throughput versus memory/FD/connection costs. Corrected earlier assumption that waiting network calls consume CPU. |
| Slice ownership design | 2 | After initial incorrect predictions, explained length versus capacity, allocation on full-capacity append, aliasing risk, and copying's memory/CPU cost. |
| Context/cancellation | 1 | Operation-scoped propagation; ignored context does not interrupt work; recognized deadline errors and partial results. Entry guard alone does not cancel later blocking work. |
| Channels/select/shutdown | 1 | Explained worker context recheck, completion/cancellation ambiguity, and send-to-closed panic. Initially missed blocked sends after collector exit and assumed select case priority; corrections delivered. Full shutdown explanation can be retrieved later in project context. |
| Mutexes/maps/race detector | 1 | Mutex does not bound concurrency or join workers; lock across Wait can deadlock; lock around network check serializes it. Map assignment aliases; different-key writes still race; overlapping reads use same mutex; reading after writers finish through Wait needs no lock. Passing race tests do not prove all schedules safe. |
| Shallow copies/channel payloads | 1 | Outer slice copy still shares inner arrays. Sending a struct copies its slice descriptor, not backing data; replacing sender's slice preserves queued data. Correctly proposed immutable handoff or separate copies to avoid races. |
| Interfaces/errors | 1 | Explained typed-nil error interface, explicit nil on success, nil receiver behavior, %w versus %v, errors.Is, and custom Unwrap. errors.As target question was deferred unanswered. |
| Uncertain health | 1 | Immediate replacement after timeout can overprovision and orphan resources. Timeout is an unknown observation; availability tradeoff of waiting was supplied by tutor. |
| Reconciliation recovery | 1 | Query provider-owned hosts after allocation succeeds but Kubernetes status is not saved. Status is the last report, not the allocation source of truth. |
| Ownership/reuse | 1 | Raised user expectations about reuse; correctly said old cleanup must check current owner UID. Tutor separated explicit adoption policy from UID ownership and supplied atomic check/delete requirement. |
| Allocation idempotency | 1 | Independently explained stable (UID, slot) EnsureHost prevents duplicates even when discovery temporarily misses the existing host. |
| Finalizers | 1 | Correctly chose finalizer-before-allocation and retaining it during provider outage to prevent orphaning. Tutor supplied user-visible Terminating-state cost; independent tradeoff explanation and cleanup-success/crash recovery remain to assess. |

Deferred/unassessed: errors.As target indirection, reslicing, compiler escape analysis, broader pointer/value receiver method sets, errgroup, pprof, actual Kubernetes API conflicts/conditions/observed generation, adoption, spec-change fencing, and all Temporal behavior. Do not turn these into a prerequisite quiz sequence.

## Implemented and verified

All code is under lab/, using only the standard library and the pinned Go 1.27.2 module.

- health.go: HostResult. health_sequential.go: CheckHostsSequential. health_concurrent.go: bounded CheckHosts, single collector, draining and joining on cancellation. Shared and concurrency-specific tests cover mixed outcomes, cancellation, actual overlap/bounds, invalid limits, and waiting for uncooperative checks.
- provider.go: fixed in-memory endpoint health/delay fixtures with cancelable waiting. examples/fleet: host-local and whole-batch deadlines. Tests cover mixed results, unknown host, cancellation, and configuration ownership.
- allocation.go: separate mutable SimulatedAllocator inventory, UID-owned hosts, atomic (UID, slot) EnsureHost, and owner-scoped repeatable ReleaseHosts. Released UIDs reject delayed allocations. Health fixtures and allocation inventory are not integrated yet.
- reconcile.go: plain-Go ReconcileHosts discovers allocations, fills missing slots, and reports observations. Discovery failure stops allocation; scale-down is explicitly unsupported. It is not a Kubernetes controller adapter.
- Fault tests cover discarded status, lost allocation response, concurrent duplicate requests, owner isolation, invalid/canceled calls, repeated cleanup, and concurrent/late allocations after cleanup.
- examples/reconcile demonstrates two existing hosts, third allocated with discarded report, retry preserving three identities, cleanup retry leaving zero hosts, and rejection of delayed provisioning. examples/slices remains available as a reference.

At session close, /opt/homebrew/bin/go test -race -count=1 -timeout=10s ./... passed from lab/ with the repo-local Go cache. Formatting and git diff --check passed. Earlier demo runs confirmed default fleet mixed results, partial batch cancellation, concurrency comparison, and allocation/cleanup recovery.

Limitations: provider state lives only in memory. Simulated controller restart keeps the provider instance alive; no actual process-restart durability is demonstrated. No CRD, controller-runtime, Kubernetes status writer/watch loop/finalizer operations, envtest assets, kind cluster, provider adoption, or Temporal workflow exists yet. Closed-owner fencing is implemented in the simulation but has not been independently explained by the learner.

## Environment continuity

See ENVIRONMENT.md for the 17 October tool/service audit and the 18 October lab checkpoint. Service availability was not rechecked today; recheck only when needed by a lesson. Prefer /opt/homebrew/bin/go and do not reuse legacy Intel envtest assets. Keep resources local; any future kind cluster must use infrastructure-tutorial and explicit context kind-infrastructure-tutorial. Native envtest assets and external Go libraries remain deferred to the Kubernetes scaffold.
