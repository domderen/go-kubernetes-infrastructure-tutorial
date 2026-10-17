# Learning progress

Last updated: 18 October 2026 (Europe/Warsaw).

## Confirmed context

- Goal: learn Go, Kubernetes, Temporal, and infrastructure for managing workloads with an interactive tutor across multiple sessions.
- Learner reports prior channel/select use and mostly having seen context.Context. Broader Go/Kubernetes/Temporal experience and available study time remain unassessed.
- Learner explicitly prefers assistant-written code, with teaching focused on reading Go, language intricacies, design review, behavior predictions, and failure diagnosis. Do not assign syntax-copying exercises.
- Learner flagged excessive isolated Go gotcha questions. Keep lessons centered on the infrastructure project and introduce language details when needed for its implementation or failures; do not treat Module 1 as an exhaustive prerequisite checklist.
- The assistant implemented sequential and bounded concurrent checkers. Learner gave a correct brief design walkthrough; next focus is applying the checker to a simulated host provider, then Kubernetes reconciliation.
- The checked-in tutorial is self-contained. Local reference PDFs are optional and ignored by Git.

## Progress

| Module | Tutorial section | Status | Evidence / next deliverable |
| --- | --- | --- | --- |
| Go concurrency, cancellation, errors, diagnostics | Module 1 | In progress | Bounded-concurrency and slice ownership design: 2 each; context, mutex/deadlock, race-detector limits, indexing, shallow copy, channel payload ownership, map synchronization, typed-nil interfaces, error wrapping/Is: 1 each. Next: apply checker to simulated hosts; defer remaining details until needed. |
| Kubernetes reconciliation and lifecycle | Module 2 | Unassessed | Sketch Reconcile; build GPUCluster with a fake provider. |
| Temporal durability, retries, determinism | Module 3 | Unassessed | Explain replay versus retry; build ProvisionCluster. |
| Workload capacity, hardware vocabulary, diagnosis | Module 4 | Unassessed | Explain boot-to-ready; diagnose a Pending GPU pod. |
| Combined system design and integration | Modules 5-6 | Not started | Define ownership and operation identity; demonstrate failure recovery. |
| Technical walkthrough and reflection | Module 7 | Not started | Demonstrate behavior, explain tradeoffs, and identify limitations. |

## Next session

1. Return to the cumulative infrastructure project: connect the bounded checker to a tiny simulated host provider and demonstrate healthy, unhealthy, and slow hosts plus cancellation.
2. Review one system behavior/failure and the implementation that causes it; then proceed to Module 2's desired state, observed state, and reconciliation. Assess Kubernetes baseline before teaching it.
3. Retain both checker implementations. Explain Go details when the project needs them; do not finish every language diagnostic before moving forward. errors.As pointer-target question was left unanswered and is deferred.
4. Keep implementation/test evidence separate from demonstrated learner understanding; leave unasked topics unassessed.

## Open setup items

Prepared 17 October 2026: installed Temporal CLI 1.9.1 and kubectl 1.37.1; updated gopls to 0.24.0 and Delve to 1.27.2. Temporal is running with persistent repo-local state. Replaced the old Podman VM with the running infrastructure-tutorial VM (4 CPUs, 8 GiB RAM, 70 GiB disk); a disposable container ran successfully. See ENVIRONMENT.md for controls and verification details.

- Obtain matching native envtest assets when the controller module is scaffolded.
- Add and pin Go project dependencies when their lessons begin.
- Create the optional kind cluster only when its exercise begins; match its Kubernetes version to kubectl.
- Recheck service availability at the next session. Interactive editor/debugger behavior has not been tested.

Observed diagnostics: learner identified resource exhaustion and connection limits; predicted a queued fast check waits while 20 slow checks occupy all slots; explained that abandoning a wait while its request continues can exceed the in-flight limit. Later correctly explained that network waits leave CPU idle, so network concurrency can exceed core count, with memory/file descriptors/connections more relevant. The earlier CPU misconception has now been retrieved correctly. Subsequent walkthrough explicitly explained the throughput/resource tradeoff, supporting score 2 for bounded-concurrency design across these answers.

Context evidence: learner described operation-scoped propagation and cancellation, correctly predicted that ignoring ctx leaves a 30-second sleep returning nil, and identified context.DeadlineExceeded from the ctx.Done() case of the supplied select example under a two-second timeout. Context/cancellation concept score: 1. Learner chose to retain per-host failures and continue the report, and correctly predicted two collected results plus cancellation error when canceled before the third host.

Review evidence: learner predicted three results with the middle host's own deadline error; top-level nil was explained. Identified unsafe concurrent append and proposed a channel/collector, but labeled the issue a deadlock; clarified that it is a data race. Correctly predicted empty results and nil error when goroutines are launched without waiting. Channel-close/shutdown mechanics remain to assess.

Channel follow-up: learner initially predicted only unused results if collector exits; explained blocked unbuffered sends, deferred Done not running, and leaked goroutines. Learner then correctly predicted a panic when a worker sends after outcomes is closed. Channel-close concept: 1; independent explanation of complete shutdown sequence remains to assess.

Select follow-up: learner initially expected first-case priority; explained uniform pseudo-random choice among ready cases. Learner then correctly explained why the worker rechecks cancellation and recognized the race between the guard and invoking check. Proposed a check-entry guard; explained that blocking work must also cooperate after entry, and corrected bare return to an error value. Learner correctly predicted nil is possible when completion/cancellation are both ready; clarified they need not become ready simultaneously. Complete shutdown reasoning remains to retrieve.

Design walkthrough: learner correctly stated that workers bound checks in progress, only the collector appends results, and workers send outcomes before channel closure during shutdown. Explained a higher limit may improve throughput while consuming more resources. Clarification to deliver: prompt completion still depends on checks cooperating with cancellation. No new implementation or tests needed for this discussion.

Mutex/testing follow-up: learner correctly noted a mutex can protect direct result appends but does not bound concurrency. Waiting for workers was clarified separately. Learner correctly identified the deadlock when caller holds the result mutex while waiting for workers that need it (concept 1). Correctly stated a passing race-detector test run does not prove absence of races in all executions (concept 1); clarified it detects races in executed paths without requiring visible corruption. Slice behavior is assessed separately below.

Slice evidence: initial length/capacity/allocation predictions were incorrect; examples demonstrated storage reuse versus allocation. Learner later correctly explained indexing uses length and full-capacity append allocates, leaving original [10]/extended [99 20]. Explained arbitrary-capacity append cannot guarantee independent storage and identified copying's memory/CPU tradeoff (ownership design 2). Heap placement was qualified via escape analysis; not assessed. Correctly predicted shared inner mutation gives 99, while replacing copied[0] leaves original at 10 because outer arrays differ (shallow copy 1). Both nested cases were added to and run in lab/examples/slices/main.go. Reslicing remains unassessed.

Channel payload ownership: initially expected sending a struct to isolate its slice elements; corrected that the copied slice descriptor still shares its backing array. Correctly predicted replacing the sender's entire slice leaves the queued label ready. Then independently explained concurrent element access can race and proposed sender stops mutating after handoff or creates an independent copy (concept 1). Clarified that continued read-only access is safe when no writer exists. No new code or tests for this discussion.

Map evidence: correctly explained assignment shares the same map; concurrent writes to different keys still need a mutex or single collector. Identified that holding the mutex during check serializes network calls; narrower locking was supplied. Correctly explained overlapping reads must use the same mutex, while a caller reading after all writers finish through Wait needs no lock (map synchronization concept 1). Clarified Wait also establishes visibility of completed writes. No code changes or tests for these predictions.

Interface evidence: correctly predicted a nil *HostError compares equal to nil while an error holding it does not. Independently explained the resulting false failure and proposed returning nil explicitly on success. Correctly predicted a method returning a constant can run with a nil pointer receiver, and explained that a nil guard in Error changes method behavior without changing the interface's non-nil status (typed-nil interface concept 1). Field dereference panic was explained, not independently predicted. Error wrapping/Is/As remain unassessed. No code changes or tests for this discussion.

Error wrapping evidence: correctly predicted direct equality false and errors.Is true for a wrapped deadline; explained that %v preserves only text while %w preserves the cause. Independently identified Unwrap() error returning Cause as necessary for the custom wrapper's chain (wrapping/Is concept 1). errors.As target indirection remains unassessed. Learner requested returning to higher-level infrastructure learning before answering it.

Implementation: shared HostResult is in lab/health.go; CheckHostsSequential is in lab/health_sequential.go; bounded CheckHosts is in lab/health_concurrent.go. A coordinator dispatches, closes jobs, waits for workers, then closes outcomes; one collector owns results, returned in arrival order. Shared tests exercise both implementations' mixed-result/cancellation contracts; concurrent-only tests cover overlap, limits, draining, and waiting for uncooperative work. No real network/provider integration yet. Keep both versions available for comparison as requested by the learner.
