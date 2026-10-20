# Learning progress

Last updated: 20 October 2026 (Europe/Warsaw), lesson in progress.

## Resume here

On 20 October the learner explicitly requested building the complete tutorial solution and running it against real Kubernetes and Temporal servers. The complete local control plane is implemented and live-tested. This request superseded the earlier small-increment implementation pace; it did not establish new learner mastery.

> Inspect the running demo: web at http://127.0.0.1:8088, Temporal UI at http://127.0.0.1:8233, provider inventory at http://127.0.0.1:9080/v1/inventory. GPUCluster/demo-gpu in namespace infrastructure-tutorial is Ready with three mock hosts. Use lab/LIVE.md and .local/live/results.json. Next learning step is a walkthrough of an actual workflow history, linking recorded activity executions to provider ownership and new observations. Avoid another chain of abstract microquestions.

Current code: integrated controller persists finalizer and workflow-ID annotation before dispatch; complete bounded ReserveHosts → EnsureImage → JoinCluster → ValidateGPU workflow; explicit failure/cancellation compensation; independent persistent HTTP mock provider; host executables/process controls; real kind cluster and Temporal service. Physical GPU actions remain simulated. The earlier e4ca8d7 checkpoint preceded this build. The learner subsequently requested committing and pushing the complete implementation to main; Git history records that checkpoint.

Latest assessment before the build: learner correctly retrieved pending on replay, then asked whether status reads should avoid activities/caching. Tutor clarified per-execution recording versus a new invocation with identical arguments; that distinction is not yet independently explained. All learning scores below remain separate from implementation/test evidence.

Preserve stable UID ownership, atomic provider keys, metadata workflow identity before dispatch, and finalizer retention until provider fencing plus workflow termination. Real deletion increments the resource generation in this cluster; never recompute an in-flight workflow ID from that deletion generation. This was found by the stopped-worker live test, reproduced in envtest with status loss, and fixed.

## Teaching preferences and baseline

- Goal: learn Go, Kubernetes, Temporal, and infrastructure by building one cumulative simulated GPU control plane.
- Assistant writes small, reviewable increments and meaningful tests. Learner reads code, reviews design, predicts system behavior, and diagnoses failures; no syntax-copying or hand-typing exercises.
- Learner flagged excessive isolated Go gotchas. Center lessons on infrastructure behavior and explain language details when the project needs them. Remaining Module 1 topics do not block Module 2.
- On 19 October, learner questioned curriculum alignment after the completion-map question. Keep the current module and end-to-end project goal visible; set optional language questions aside when they interrupt the lesson's direction.
- Go: reports prior channel/select use and mostly having seen context.Context; demonstrated concepts are recorded below.
- Kubernetes: mainly workload deployment, some CRD/controller use, limited experience writing controllers; operator terminology was explained, not independently assessed.
- Temporal: reports having built something before, but concepts remain vague. Basic completion/retry diagnostics are recorded below; no language/production context inferred. Available study time remains unassessed.
- Keep both sequential and concurrent checker implementations for comparison.

## Curriculum status

| Module | Status | Next deliverable |
| --- | --- | --- |
| 1: Go concurrency, cancellation, errors, diagnostics | In progress; checker and simulations implemented | Revisit remaining language/diagnostic topics when needed by the project. |
| 2: Kubernetes reconciliation and lifecycle | Learning in progress; integrated controller runs on kind, with envtest and live recovery coverage | Walk through persisted identity, status, and deletion from actual evidence. |
| 3: Temporal durability and retries | Learning in progress; full workflow, compensation, real process restart, cancellation and history replay verified | Inspect actual history and explain replay versus a new activity invocation. |
| 4: Workload capacity and diagnosis | Unassessed | Host lifecycle, capacity, scheduling, and failure diagnosis. |
| 5–6: Combined design and integration | Complete teaching implementation and live tests; learner design understanding remains to assess | Explain controller/workflow/provider ownership through the running system. |
| 7: Technical walkthrough | Not started | Demonstrate behavior and explain failure/recovery/tradeoffs. |

No entire learning module is complete. Implementation/test results are not proof of learner understanding.

## Demonstrated understanding

Scores: 0 = cannot yet explain; 1 = correct concept; 2 = explains a failure case and a tradeoff. Unassessed is distinct from 0.

| Topic | Score | Observed evidence |
| --- | --- | --- |
| Bounded concurrency design | 2 | Explained resource exhaustion, slot occupancy, actual in-flight limits, collector ownership/shutdown, and throughput versus memory/FD/connection costs. Corrected earlier assumption that waiting network calls consume CPU. |
| Slice ownership design | 2 | After initial incorrect predictions, explained length versus capacity, allocation on full-capacity append, aliasing risk, and copying's memory/CPU cost. |
| Context/cancellation | 1 | Operation-scoped propagation; ignored context does not interrupt work; recognized deadline errors and partial results. Correctly explained time.Sleep delays cancellation until sleep finishes in the imaging poll loop. Entry guard alone does not cancel later blocking work. |
| Channels/select/shutdown | 1 | Explained worker context recheck, completion/cancellation ambiguity, and send-to-closed panic. Initially missed blocked sends after collector exit and assumed select case priority; corrections delivered. Full shutdown explanation can be retrieved later in project context. |
| Mutexes/maps/race detector | 1 | Mutex does not bound concurrency or join workers; lock across Wait can deadlock; lock around network check serializes it. Map assignment aliases; different-key writes still race; overlapping reads use same mutex; reading after writers finish through Wait needs no lock. Passing race tests do not prove all schedules safe. |
| Shallow copies/channel payloads | 1 | Outer slice copy still shares inner arrays. Sending a struct copies its slice descriptor, not backing data; replacing sender's slice preserves queued data. Correctly proposed immutable handoff or separate copies to avoid races. |
| Interfaces/errors | 1 | Explained typed-nil error interface, explicit nil on success, nil receiver behavior, %w versus %v, errors.Is, and custom Unwrap. errors.As target question was deferred unanswered. |
| Uncertain health | 1 | Immediate replacement after timeout can overprovision and orphan resources. Timeout is an unknown observation; availability tradeoff of waiting was supplied by tutor. |
| Reconciliation recovery | 1 | Query provider-owned hosts after allocation succeeds but Kubernetes status is not saved. Status is the last report, not the allocation source of truth. |
| Ownership/reuse | 1 | Raised user expectations about reuse; correctly said old cleanup must check current owner UID. Tutor separated explicit adoption policy from UID ownership and supplied atomic check/delete requirement. |
| Allocation idempotency | 1 | Independently explained stable (UID, slot) EnsureHost prevents duplicates even when discovery temporarily misses the existing host. |
| Finalizers | 1 | Correctly chose finalizer-before-allocation, retention during provider outage, and success for repeated cleanup of absent hosts. Identified orphaned hosts if finalizer removal is saved before cleanup. Needed clarification of retained API object versus absent hosts, and cleanup code versus key removal. Full recovery explanation and independent tradeoff remain to retrieve. |
| Generation/observed generation | 1 | After corrections about schema version and incrementing, correctly said a second pass over unchanged generation 7 increases neither field. Full explanation and a concurrent spec-update failure case remain to retrieve. |
| Status conditions | 1 | After initial uncertainty and explanation, correctly said current count is unknown after timeout and only the prior count of 3 is known; reporting 0 would be unjustified. Implementation is tutor work; condition-field details and tradeoffs remain to assess. |
| Reconcile triggers | 1 | Named user/provider changes and desired/actual divergence. Tutor clarified that events/timers queue the key before the gap is evaluated, provider signals must be wired explicitly, and allocation polling does not imply health checks. Complete queue explanation remains to retrieve. |
| Provider polling design | 2 | Explained that a shorter interval means more calls/resource use and could overwhelm the provider, in exchange for potentially faster failure detection. |
| Temporal recorded-completion recovery | 1 | Explained reuse of durably recorded ReserveHosts after worker replacement. On 20 October correctly answered pending again for an illustrative completed status activity during replay despite the provider now saying done. Tutor clarified workflow replay versus activity dispatch and fresh observations requiring new activity executions. |
| Temporal replay determinism | 0 | On 20 October initially unsure; after a concrete hint, correctly predicted a live pending/done branch would be skipped and later correctly retrieved the recorded pending result for an activity-based lookup. Tutor supplied the command/history mismatch explanation; full determinism concept not yet independently explained. |
| Temporal retry recovery | 1 | Proposed owned-host rediscovery; correctly said ListHosts(B) cannot return H1 owned by A. On 20 October correctly selected only H2's activity for recovery after a lost image response and proposed finding its already-started job. Tutor supplied the atomic same-key ensure refinement, including uncertain lookup outcomes. Earlier stable-key guarantee and activity retry versus workflow restart needed clarification. Full independent failure/tradeoff explanation remains to retrieve. |
| Temporal activity timeouts | 2 | Identified StartToClose as one attempt and 2s remaining after 28s queueing under a 30s budget. Explained shorter timeout gives faster retry after failure but risks excess requests while responses remain pending. Tutor supplied healthy-slow-call example and distinction from backoff/attempt caps. |
| Temporal external timeout uncertainty | 1 | Correctly said a host may still be created after Temporal's timeout. Tutor connected this to cooperative cancellation, overlapping attempts, and stable provider keys. Cancellation/heartbeat implementation remains unassessed. |
| Temporal terminal errors | 1 | Chose immediate stop for ErrOwnerReleased because the UID has no further work. Tutor refined this to no further allocations, and explained the non-retryable classification overrides remaining attempt/time budgets. |
| Temporal heartbeat recovery | 1 | Associated worker disappearance with retry and correctly predicted a helper's continuing heartbeats would not detect stalled imaging. Tutor corrected “missing activity” to missing heartbeats, distinguished target-host health, and supplied the independent attempt-limit safeguard. Independent tradeoff explanation remains to retrieve. |
| Temporal heartbeat checkpoint payload | 1 | Independently proposed job ID and host ID so the next attempt can find existing imaging work. Payload delivery/throttling limits remain to retrieve. |
| Temporal missing-checkpoint recovery | 1 | Proposed an idempotency key across retries, wondering whether job/activity IDs could match. Tutor clarified the preexisting provider key versus returned job ID, and supplied owner/operation scoping and immutable request parameters. |
| Image operation request binding | 1 | Initially did not follow the guard; after the concrete example, identified intentional os-v2 imaging as a new operation. Then correctly kept image-2 on a lost-response retry because the outcome is uncertain. Stored/incoming request meaning and owner-ID preservation were tutor-supplied. |

Deferred/unassessed: errors.As target indirection, reslicing, compiler escape analysis, broader pointer/value receiver method sets, errgroup, pprof, actual Kubernetes API conflicts/cache consistency, adoption, spec-change fencing, and Temporal request-key conflict reasoning, retry-policy/error-classification details, stale heartbeat checkpoints and throttling, cancellation, versioning, and integration. Do not turn these into a prerequisite quiz sequence.

## Implemented and verified

- Original Go checker, simulated health endpoints, allocation/reconciliation and slice examples remain available with their tests. The allocation-only Kubernetes adapter remains a comparison implementation; the live executable uses ProvisioningReconciler.
- GPUCluster API: immutable nodeCount/imageVersion/gpuClass/maxParallel; defaults and admission bounds; workflow/run IDs, phase, host counts, conditions and error status. Finalizer and workflow-ID annotation persist before dispatch. Status-only loss recovers the retained operation. Deletion fences the provider UID, cancels the original workflow, and waits for termination before removing protection.
- Temporal: ReserveHosts then bounded per-host image/join/validate; stable intent/step/slot keys. WaitForCancellation and joined SDK workers precede disconnected bounded ReleaseHosts compensation. Production worker registers all activities. Failed cleanup preserves the original error and reports the cleanup failure.
- mockprovider: separate HTTP process with atomic JSON commits before responses, UID ownership/fencing, host/image/action idempotency, elapsed-time imaging, simulated membership/validation, persisted fault injection, inventory counters/audit trail. Actual dropped HTTP connections exercise ambiguous completions.
- Runtime: provider, worker, controller and offline/download replay commands; make up/down/status/demo/live-test; explicit dedicated kubeconfig/context and tracked full-command process safety. Kubernetes serves a real web Deployment; simulated hosts are not registered as real Nodes.
- Checked-in 47-event real-server history replays during unit tests with no activities/provider registered. An isolated activity-name breaking-change mutation produced TMPRL1100 nondeterminism; original sources stayed untouched. Process-safety tests also reject an executable-only PID-matching mutation.

Verification on 20 October: fresh make test passed all Go packages with race detector, native envtest and three Python process-control tests. Twelve live checks passed against kind v1.37.0 and Temporal server 1.32.0: Ready count, lost responses without duplicate logical resources, status wipe/same run, spec rejection, provider process restart, worker OS-process crash/original jobs, actual-history replay/no provider effects, terminal validation failure/compensation, live-worker deletion, stopped-worker fencing/finalizer wait, real cancellation acknowledgment before compensation, and cleanup leaving the Ready demo. Evidence and histories: .local/live/. Independent review findings were resolved and scoped re-review found no remaining material issues.

Important fixes: controller CLI global kubeconfig flag collision; distinct controller manager name in tests; deletion generation bump originally selected a nonexistent workflow and removed the finalizer early. The last bug was exposed by the stronger live test and covered with real envtest regression including a status wipe. Current metadata identity survives that failure. These are implementation results, not learner accomplishments.

Limits: mock hardware only; single provider writer/JSON state; no physical-job cancellation, GPU drivers, real host joining, automatic physical repairs, adoption/resizing, global hardware action bound, or production deployment. Ready reports completed simulated lifecycle and matching inventory. Temporal history retention plus simultaneous removal of Kubernetes identity/status evidence can remove workflow completion evidence; provider keys remain side-effect guards. Missing deleting legacy identity retains the finalizer.

## Environment continuity

See ENVIRONMENT.md and lab/LIVE.md. The dedicated infrastructure-tutorial kind cluster, Temporal dev service, provider/controller/worker host processes and web port-forward were left running. State/logs/PIDs/history are under ignored .local/. Use only explicit kind-infrastructure-tutorial context and .local/kubeconfig. Homebrew Go and per-command Command Line Tools remain necessary; no global shell/Xcode settings changed. Recheck service health at the next session.
