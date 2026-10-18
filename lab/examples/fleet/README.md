# Simulated fleet health scan

This is the first provider integration in the cumulative infrastructure lab.
It models host health endpoints in memory; it does not allocate machines or
run a Kubernetes controller yet.

From `lab/`, run:

```sh
go run ./examples/fleet
go run ./examples/fleet -limit=1
go run ./examples/fleet -batch-timeout=25ms
```

The configured fleet:

| Host | Response delay | Endpoint response |
| --- | --- | --- |
| gpu-01 | 50 ms | Healthy |
| gpu-02 | 100 ms | GPU diagnostic failed |
| gpu-03 | 2 seconds | Healthy, if given enough time to respond |
| gpu-04 | 50 ms | Healthy |

By default, two checks can run at once, each has 250 ms to respond, and
the whole scan has two seconds. The normal run collects all four results:
two healthy, one diagnostic failure, and one unknown because its check
timed out. A host-local deadline does not end the batch. Arrival order and
elapsed time can vary with scheduling.

The short batch deadline demonstrates partial results: started checks stop
waiting, queued hosts are skipped, and the batch returns its deadline error.
Canceled checks do not change the simulated hosts' configured health.

## Responsibilities

- `SimulatedProvider` owns a fixed copy of endpoint behavior and answers
  health requests. Waiting cooperates with request cancellation.
- `CheckHosts` bounds concurrent calls and collects outcomes. It accepts
  `provider.CheckHealth` through its existing check function parameter.
- The example configures fleet policy: concurrency, per-host deadline,
  and whole-scan deadline. Its callback adds a child context for each host.

A timeout is a failed observation, not proof that the machine is broken.
This scan is a snapshot. The next lesson introduces the controller that
compares observed state with desired capacity and decides what to do next.
