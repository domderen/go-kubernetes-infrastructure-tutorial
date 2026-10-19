# Go and Kubernetes infrastructure tutorial

A hands-on tutorial for learning Go, Kubernetes controllers, and Temporal by building a small infrastructure control plane for managing workload capacity.

The cumulative lab uses a simulated host provider. It introduces bounded health checks, resource reconciliation, durable provisioning workflows, and recovery from partial failures. GPU capacity is a concrete example; the exercises run locally without GPUs or a cloud account.

## Learning path

1. **Go fundamentals:** concurrency limits, cooperative cancellation, shared state, errors, and diagnostics.
2. **Kubernetes controllers:** desired state, reconciliation, custom resources, conditions, finalizers, and deletion.
3. **Temporal workflows:** durable orchestration, deterministic replay, activity retries, timeouts, and compensation.
4. **Workload infrastructure:** host lifecycle, capacity, scheduling constraints, health, and repair.
5. **Integration:** operation identity, ownership, idempotency, and recovery across system boundaries.
6. **Technical walkthrough:** demonstrate the system, inject failures, and explain design tradeoffs.

Start with the [tutorial guide](reference/guide.txt) and [official documentation links](reference/links.md).

## Workspace

- `lab/`: the cumulative project; separate sequential (`health_sequential.go`) and bounded concurrent (`health_concurrent.go`) host-health checkers, with shared behavior and concurrency tests.
- `lab/provider.go` and [the fleet demo](lab/examples/fleet/README.md): a simulated host provider with healthy, unhealthy, slow, and canceled health checks (`go run ./examples/fleet` from `lab/`).
- `lab/allocation.go`, `lab/reconcile.go`, and [the reconciliation demo](lab/examples/reconcile/README.md): UID-owned allocations, recovery after a controller loses its report, repeatable cleanup, and rejection of delayed provisioning (`go run ./examples/reconcile` from `lab/`).
- [The Kubernetes controller](lab/controller/README.md): a typed GPUCluster API, finalizers, allocation conditions, watch/poll scheduling, and failure-recovery tests with a real manager against a temporary API server (`make test` from `lab/`).
- [The Temporal workflow and imaging activity](lab/provisioning/README.md): a reservation workflow plus a separately tested imaging activity with stable provider identity, polling/heartbeats, cancellation, and bounded retry tests.
- [Imaging job identity](lab/imaging.go): the simulated provider recovers one job by a stable owner/operation key, even when the returned job ID is lost. Explicit completion events let tests observe success without real imaging.
- `lab/examples/slices/`: runnable examples comparing length, capacity, backing-array reuse, and nested shallow copies (`go run ./examples/slices` from `lab/`).
- [Learning progress](notes/PROGRESS.md): demonstrated understanding and the next exercise.
- [Session log](notes/SESSION_LOG.md): continuity across learning sessions.
- [Environment audit](notes/ENVIRONMENT.md): observed tool versions and local setup commands.
- [Tutoring instructions](AGENTS.md): how an assistant should support the learner.

Local reference PDFs are ignored by Git and are not required to follow the checked-in tutorial.

## Continue learning

Open this directory and ask to continue the tutorial. The tutor reads the saved notes, checks the learner's baseline, and works through one exercise at a time.

```sh
cd ~/dev/go-kubernetes-infrastructure-tutorial
```

Module 1's checker and simulated fleet are implemented. Module 2 includes a typed GPUCluster controller with finalizers, allocation conditions, watches, and periodic inventory checks. Envtest covers direct reconciliation and a real manager responding to API changes. Module 3 has a reservation-only workflow plus a separately tested EnsureImage activity that polls and heartbeats against simulated job completion. Wiring imaging into the workflow, real imaging, joining, persisted-history/worker-restart exercises, and controller integration are later increments. Use the progress notes for the current question. Provider state lives only in memory; the manager and workflow currently run inside tests, and no standalone controller or kind cluster is running. The assistant writes small implementations; the learner practices reading, reviewing, predicting behavior, and explaining tradeoffs. Go language details are introduced when the project needs them. No learning module has been completed yet.

The Go toolchain, Temporal CLI, kubectl, and updated Go editor/debugger tools are installed. Local Temporal and a fresh Podman VM were started and verified on 17 October 2026; see the environment notes for service controls. Pinned Kubernetes dependencies and matching native envtest assets were added on 18 October, and the Temporal Go SDK/test suite on 19 October. A full kind cluster is optional; the current tests need no container engine or running Temporal server.
