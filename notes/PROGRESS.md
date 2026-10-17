# Learning progress

Last updated: 17 October 2026 (Europe/Warsaw).

## Confirmed context

- Goal: learn Go, Kubernetes, Temporal, and infrastructure for managing workloads with an interactive tutor across multiple sessions.
- The learner's experience and available study time are not yet assessed.
- Workspace setup, machine audit, and the requested tool/service preparation are complete. No learning module or project exercise has been attempted.
- The checked-in tutorial is self-contained. Local reference PDFs are optional and ignored by Git.

## Progress

| Module | Tutorial section | Status | Evidence / next deliverable |
| --- | --- | --- | --- |
| Go concurrency, cancellation, errors, diagnostics | Module 1 | Unassessed | Answer self-check questions; build bounded CheckHosts with tests. |
| Kubernetes reconciliation and lifecycle | Module 2 | Unassessed | Sketch Reconcile; build GPUCluster with a fake provider. |
| Temporal durability, retries, determinism | Module 3 | Unassessed | Explain replay versus retry; build ProvisionCluster. |
| Workload capacity, hardware vocabulary, diagnosis | Module 4 | Unassessed | Explain boot-to-ready; diagnose a Pending GPU pod. |
| Combined system design and integration | Modules 5-6 | Not started | Define ownership and operation identity; demonstrate failure recovery. |
| Technical walkthrough and reflection | Module 7 | Not started | Demonstrate behavior, explain tradeoffs, and identify limitations. |

## Next session

1. Briefly establish recent Go/Kubernetes/Temporal experience and available study time.
2. Start with one Go self-check question from Module 1, then adapt the depth to the learner's answer.
3. Agree on CheckHosts behavior, especially unhealthy host as a result versus fatal error, then implement the smallest useful increment together.
4. Record the learner's answer, feedback, and test evidence here or in the session log. Leave unasked topics unassessed.

## Open setup items

Prepared 17 October 2026: installed Temporal CLI 1.9.1 and kubectl 1.37.1; updated gopls to 0.24.0 and Delve to 1.27.2. Temporal is running with persistent repo-local state. Replaced the old Podman VM with the running infrastructure-tutorial VM (4 CPUs, 8 GiB RAM, 70 GiB disk); a disposable container ran successfully. See ENVIRONMENT.md for controls and verification details.

- Obtain matching native envtest assets when the controller module is scaffolded.
- Add and pin Go project dependencies when their lessons begin.
- Create the optional kind cluster only when its exercise begins; match its Kubernetes version to kubectl.
- Recheck service availability at the next session. Interactive editor/debugger behavior has not been tested.

No scores, misconceptions, or completed exercises have been recorded yet.
