# Local infrastructure control plane

The user explicitly requested the complete tutorial implementation and live verification on real Kubernetes and Temporal servers. This supersedes the earlier one-question-at-a-time implementation pace. Preserve the existing lab and uncommitted lesson work. No cloud resources or physical GPUs are involved.

## Architecture

Run a dedicated kind cluster named infrastructure-tutorial on the existing Podman VM, with kubeconfig under .local and explicit kind-infrastructure-tutorial context. Run Temporal's persistent local development server. Host Go processes provide the mock provider HTTP API, Temporal worker, and controller; this avoids confusing container-to-host networking and keeps debugging direct. Kubernetes remains a real cluster with its scheduler, controllers, API server, and a sample workload.

The integrated controller persists its finalizer before starting work, derives one stable workflow ID from resource UID/generation, observes existing workflows after lost status writes, and projects progress/results into GPUCluster status. It never allocates hosts itself. Spec is immutable for this lab. The Temporal workflow alone sequences ReserveHosts, then bounded per-host EnsureImage → JoinCluster → ValidateGPU, and explicit cleanup on failure/cancellation. The separate mock provider owns authoritative simulated allocations, image jobs, memberships, validation, and closed owner UIDs. Persist its state atomically to a JSON file before responding. HTTP response loss must preserve the committed side effect and stable key. Worker/provider process restarts must not duplicate resources.

JoinCluster and ValidateGPU are simulated provider actions, not real GPU nodes or drivers. Do not register imaginary kubelet Nodes or claim GPU hardware validation. Run an ordinary Kubernetes Deployment separately to prove the cluster actually schedules workloads.

## Contracts

Existing lab.HostAllocation, HostAllocator, ImageRequest, ImageJob remain intact. Add lab.HostOperation with OwnerUID, OperationID, HostID, GPUClass strings; shared ErrUnsupportedGPUClass. Provider supplies JoinCluster(context.Context, lab.HostOperation) error, ValidateGPU(context.Context, lab.HostOperation) error and ReleaseHosts(context.Context,string) error, alongside existing allocation/image interfaces.

provisioning.Request gains GPUClass string and MaxParallel int (zero defaults to sequential for old tests). Worker registers all production activities. Integrated Kubernetes spec supplies imageVersion, gpuClass, maxParallel with defaults; status exposes workflowID, runID, phase, readyHosts, lastError, host inventory, observedGeneration, and standard conditions. Preserve old allocation-only adapter/tests as a comparison; the executable uses the integrated reconciler.

mockprovider.New(path string, imageDelay time.Duration) (*Server,error), Server.Handler() http.Handler, and mockprovider.NewClient(baseURL string) *Client are public entrypoints. Client implements all provider methods. HTTP health/inventory/fault endpoints support inspection and deterministic lost-response/terminal-failure demonstrations. Persistent state is owned by one provider process, protected by a mutex and atomic file replacement.

## Safety and recovery

Deletion first closes/releases the provider UID, then requests workflow cancellation and waits for a terminal workflow before finalizer removal. This fences delayed activity calls, survives controller retries, and preserves already-accepted image-job history as uncertain external work. Release is idempotent. Permanent invalid requests/unsupported GPU class stop retries. Cleanup is explicit, bounded, and observable; no automatic database rollback is claimed. Successful workflow IDs cannot be restarted by reconciliation. Ready denotes completed simulated lifecycle, not physical GPU health.

## Verification

Unit/race tests cover provider persistence, duplicate keys, lost responses, closed-owner fencing, bounded workflow fan-out, activity retries/terminal failures/compensation, finalizer-first dispatch, duplicate dispatch, status-write recovery and deletion ordering. Preserve existing envtest checks. Live verification must create GPUCluster and observe Ready, inject lost responses without duplicate logical hosts/jobs, kill/restart the worker with provider state preserved, replay downloaded history, reject spec changes, surface terminal failure, and delete during provisioning with no orphan hosts. Save run IDs/results locally and document reproducible controls. Leave a Ready sample and services running for the learner to inspect. No commit/push is requested by this new task.
