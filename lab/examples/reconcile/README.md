# Allocation reconciliation and crash recovery

From `lab/`, run:

```sh
go run ./examples/reconcile
```

The simulated provider starts with two hosts owned by cluster UID A. One
reconciliation requests a third. Its report is deliberately discarded to
represent a controller crash before Kubernetes status was saved. The next
pass discovers all three existing hosts and reports them without creating a
fourth. It then releases that UID's hosts twice, demonstrating cleanup retry,
and rejects a delayed allocation for the released UID.

## Ownership and operation identity

Each host has an owner UID and a logical slot (1, 2, 3). An allocation
request uses the stable `(owner UID, slot)` key. `EnsureHost` atomically
returns an existing allocation for that key or creates one. This contract
also prevents duplicate allocations if two callers request the same slot.

`ReconcileHosts` first reads the provider inventory. Failed discovery ends
the pass with an error; it is not interpreted as an empty inventory. A
subsequent attempt can retry using the same owner and slot keys. Tests inject
a lost response after successful allocation and verify the retry preserves
the original host.

## Cleanup contract

`ReleaseHosts` removes only the specified UID's allocations. Repeating it
succeeds even when nothing remains, which lets the controller retry after a
lost response. Under the same provider lock, it records that the UID has been
released. Later `EnsureHost` requests for that UID return `ErrOwnerReleased`,
preventing delayed provisioning from recreating hosts after cleanup.

Tests cover UID isolation, retry, canceled cleanup, invalid ownership input,
and concurrent allocation/release. This simulates a provider guarantee; a
real provider integration needs an equivalent durable mechanism.

## Scope

The allocator is a separate in-memory simulation of allocation inventory;
the fleet example's endpoint health fixtures remain separate. Allocated
does not imply healthy or ready.

This exercise keeps the provider instance alive across simulated controller
restarts. It does not demonstrate persistence across process restarts.
`ReconcileHosts` is business logic for a future Kubernetes controller; no
CRD, API server, status writer, or watch loop has been installed yet.

This increment fills missing slots. Scale-down returns an explicit error.
The provider cleanup operation is implemented; the Kubernetes deletion branch,
finalizer persistence/removal, adoption, concurrent spec changes, and status
conditions remain later increments. Ownership transfer is not implemented.
