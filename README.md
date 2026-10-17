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

- `lab/`: the cumulative project; currently empty apart from a directory placeholder.
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

The next step is a short Go baseline check, followed by a bounded concurrent host-health checker. No learning module has been completed yet.

The initial audit found the Go toolchain ready. Temporal CLI and modern Kubernetes test tooling need setup for later lessons. A full kind cluster is optional; begin with Go unit tests, then use Temporal's test suite and Kubernetes envtest.
