# Go and Kubernetes infrastructure tutorial

A hands-on tutorial for learning Go, Kubernetes controllers, and Temporal by building a small infrastructure control plane for managing workload capacity.

The cumulative lab uses a simulated host provider. It introduces bounded health checks, resource reconciliation, durable provisioning workflows, and recovery from partial failures. GPU capacity is a concrete example; the exercises run locally without GPUs or a cloud account.

## Working local system

The complete provisioner runs against a real local kind cluster and Temporal server. See [the live lab](lab/LIVE.md) for startup, inspection, crash/recovery checks, and controls. The running demo is available at [localhost:8088](http://127.0.0.1:8088), with [Temporal history at localhost:8233](http://127.0.0.1:8233).

From `lab/`, run `make up`, `make demo`, and `make live-test`. GPUCluster provisioning now includes reservation, bounded imaging/joining/validation, persistent mock-provider state, Kubernetes status projection, and coordinated deletion.

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
- [The Temporal provisioning workflow](lab/provisioning/README.md): reservation followed by bounded imaging, joining, and validation, with stable provider identity, heartbeats, cancellation, compensation, and real-history replay tests.
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

The cumulative implementation now connects GPUCluster reconciliation to a complete Temporal workflow and a persistent HTTP mock provider. It reserves hosts and runs bounded imaging, simulated joining, and GPU validation, with explicit compensation and fenced deletion. A dedicated kind cluster, Temporal service, host controller/worker/provider, and Kubernetes web workload form the live lab. Real-server tests cover response loss, process crashes, history replay, status recovery, terminal errors, and cancellation. Implementation is separate from demonstrated learner understanding; the progress notes preserve that distinction. The remaining lessons can use this working system for code reading and failure walkthroughs.

The Go toolchain, Temporal CLI, kubectl, kind, and Podman are installed. See the environment notes and live-lab guide for verified versions and process controls. Unit/envtest checks run without the persistent cluster or Temporal service; `make live-test` explicitly uses both running servers.
