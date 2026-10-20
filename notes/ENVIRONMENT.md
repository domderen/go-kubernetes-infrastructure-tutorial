# Environment audit

Checked: 17 October 2026 tool/service audit; 18–19 October adapter/SDK setup; 20 October complete live control plane, Europe/Warsaw. This is a snapshot, not a promise that services remain running across sessions.

## Readiness

Complete live lab verified on 20 October: dedicated kind Kubernetes cluster Ready, existing Temporal server healthy, persistent HTTP mock provider, worker/controller host processes, and Kubernetes web workload. Fresh make test and twelve real-server checks passed. Physical GPU actions remain simulated; no cloud account, physical GPUs, CUDA, drivers, or bare metal are required. Historical observations below retain their original dates; current controls are in lab/LIVE.md.

### 20 October 2026 - Live runtime

- Go 1.27.2 darwin/arm64; kind 0.33.0; kubectl 1.37.1; Temporal CLI 1.9.1, embedded server 1.32.0, UI 2.54.1 rechecked.
- Existing Podman infrastructure-tutorial VM was running and reused. Created dedicated kind infrastructure-tutorial using kindest/node:v1.37.0; real control-plane Node Ready, containerd 2.3.4. Kubeconfig is repo-local .local/kubeconfig, explicit context kind-infrastructure-tutorial; no default user kubeconfig rewrite.
- Existing Temporal process at 127.0.0.1:7233 reused; operator cluster health reports SERVING, UI HTTP 200. Persistent .local/temporal/dev.db preserved.
- make up builds lab/bin/{provider,worker,controller,replay}; script tracks host processes/logs under .local/runtime. Provider HTTP is 127.0.0.1:9080 with state .local/provider/state.json. Controller probes 127.0.0.1:8091; Kubernetes web port-forward 127.0.0.1:8088.
- Applied GPUCluster CRD, namespace infrastructure-tutorial, web ConfigMap/Deployment/Service, and Ready sample GPUCluster/demo-gpu. Twelve live checks cover real process crashes/restarts, response loss, status recovery, replay, terminal failure, active/stopped-worker deletion and cancellation acknowledgment ordering. Histories/results saved under .local/live.
- Fresh full make test passed with race detector, native envtest and Python process-control tests. Reused per-command DEVELOPER_DIR=/Library/Developer/CommandLineTools; no global Xcode/license/shell changes. Runtime's zap logging adds existing transitive modules to direct/indirect Go requirements; no new server/tool installation needed.
- Services, cluster, three mock demo hosts and web workload left running. make down stops host lab processes/port-forward while preserving Temporal, kind and state. See lab/LIVE.md for full controls; recheck availability after reboot/session change.

### Before installation, 17 October 2026 (historical)

The earlier audit found Temporal CLI and modern kubectl missing, older gopls/Delve versions, and only legacy Intel envtest binaries. The follow-up initially could not inspect Podman's lockfile inside the sandbox. Installation and service setup were subsequently explicitly authorized and performed with elevated access. The lab was still a placeholder at that historical check; its current implementation is described in PROGRESS.md.

## Machine

- macOS 26.6, darwin/arm64 (Apple Silicon).
- 32 GiB RAM, 10 logical CPUs; approximately 135 GiB available disk when checked.
- Homebrew installed at `/opt/homebrew`.

## Installed tools and checks

| Item | Observed state | What was verified |
| --- | --- | --- |
| Go | 1.27.2 at /opt/homebrew/bin/go | darwin/arm64; CGO_ENABLED=1; compiled and ran a standard-library test with the race detector. |
| Git | 2.56.0 | Version command succeeded. |
| C compiler / build tools | Apple clang 17.0.0; GNU Make 3.81 | Version commands and Go race build succeeded; Xcode developer directory selected. |
| Editor | VS Code 1.142.0 with golang.go extension; VSCodium also installed | VS Code CLI version and installed extension listing. |
| gopls | Updated 0.19.1 → 0.24.0 at ~/go/bin/gopls | Installed with Homebrew Go; version command succeeds; native arm64. Interactive editor behavior not tested. |
| Delve | Updated 1.21.2 → 1.27.2 at ~/go/bin/dlv | Installed with Homebrew Go; version command succeeds; native arm64. Interactive debugging not tested. |
| Kubebuilder | 4.16.0 | Version command succeeded; project not scaffolded yet. |
| kind | 0.33.0 | Cluster listing using Podman succeeds; no kind clusters found. |
| Podman | Client/server 6.1.4; Podman Desktop installed | New infrastructure-tutorial VM running; linux/arm64 guest; disposable hello-world container succeeded. |
| Helm | 4.3.1 | Version command succeeded; not required for the first exercises. |
| kubectl | Installed 1.37.1 at /opt/homebrew/bin/kubectl | Client version command succeeds; native arm64. No Kubernetes API server/cluster created yet. |
| envtest assets | Native Kubernetes 1.37.0 in lab/bin/k8s/1.37.0-darwin-arm64 (18 October) | kube-apiserver and etcd are Mach-O arm64; real API-server lifecycle tests passed. Makefile pins setup-envtest. Legacy Intel 1.19.2 / etcd 3.4.9 assets were left untouched and are not used. |
| Temporal CLI | Installed 1.9.1 at /opt/homebrew/bin/temporal; embedded server 1.32.0, UI 2.54.1 | Native arm64; local service health SERVING, default namespace registered, web UI HTTP 200. |
| Go dependency access | Module downloads succeeded on 18–19 October | controller-runtime, Kubernetes libraries, pinned generation/envtest tools, and Temporal SDK v1.50.0 downloaded and compiled. Combined race-detector suite passed. |

Docker, Minikube, and Colima were not found on the checked PATH. They are not additional requirements for the proposed route: use the existing Podman installation if choosing kind, and use Temporal CLI for the local Temporal service.

The lab module uses Go 1.27.2. Its checker/provider code uses the standard library; the Kubernetes adapter pins controller-runtime v0.25.1 and Kubernetes libraries v0.37.0 in lab/go.mod, with go.sum tracked alongside it. Temporal SDK v1.50.0 (API dependency v1.63.5) was added for the reservation workflow. Controller-gen v0.23.0 and a pinned setup-envtest revision are installed only in ignored lab/bin. Native envtest 1.37.0 matches the libraries' minor version; a 1.37.1 darwin/arm64 archive was not available. Direct use of errgroup remains deferred until its lesson.

## Verification evidence

An isolated temporary module containing a standard-library goroutine/channel test was run with:

```text
go test -race -count=1 ./...
```

The command succeeded with exit 0. This verifies compilation, C toolchain/race-runtime availability, and execution on this Mac. It does not implement or validate the tutorial's health checker. The temporary module was removed.

Replaced podman-machine-default, including its VM storage, with explicit user authorization. The new infrastructure-tutorial VM uses applehv, 4 CPUs, 8 GiB RAM, and a 70 GiB disk. Containers run rootfully inside the VM; the default connection is infrastructure-tutorial-root. Confirmed Running=true, Starting=false, matching client/server versions 6.1.4, and successful arm64 hello-world execution. Removed the test image; container, image, and volume listings are empty. Podman-backed kind listing succeeds with no clusters.

The command runner initially terminated VM helpers after startup, leaving partially initialized disks and stale sockets. Recreated the clean disk and started through a session-only macOS job, local.infrastructure-tutorial.podman-start, with AbandonProcessGroup=true. Definition/logs are in .local/podman/. This is registered for the current login session, not installed as a login startup agent. The VM remains available after the start job exits.

Temporal runs as a detached local process, bound to 127.0.0.1:7233 with UI on 127.0.0.1:8233. Persistent database: .local/temporal/dev.db; log: .local/temporal/server.log; PID: .local/temporal/server.pid. Database creation, service health, namespace registration, and UI response were checked. No workflow has been created; persistence across service restart has not been exercised. All .local files are ignored by Git. No automatic Temporal startup at login was configured.

## Service controls and remaining setup

### 18 October 2026 - Lab checkpoint before Kubernetes adapter (historical)

The lab still uses only the standard library. Sequential/concurrent checkers,
simulated health endpoints, allocation reconciliation, and owner-scoped cleanup
are implemented, with runnable fleet and reconciliation examples. The complete
Go test suite passed with the race detector using /opt/homebrew/bin/go and the
repo-local .local/go-build cache. No new tools or external dependencies were
installed today. Temporal/Podman availability was not rechecked; their last
verified service state below is from 17 October. Native envtest assets,
controller-runtime/Kubernetes libraries, CRDs, and a running controller remain
for the next scaffold. The allocation simulator has no persistent storage.

### 18 October 2026 - Kubernetes adapter setup

Added the typed GPUCluster resource/CRD and controller adapter. The Makefile
generates deepcopy/CRD files and obtains native Kubernetes 1.37.0 assets.
make test passed with integration tag, race detector, and a temporary loopback
API server/etcd; the tests stopped that control plane afterwards. There is no
manager/watch loop, persistent Kubernetes API server, or kind cluster. The
provider remains in memory. Temporal/Podman were not rechecked or changed.

The active xcode-select path is /Applications/Xcode.app/Contents/Developer;
make now prompts for Xcode license acceptance. The existing Command Line Tools
work with a per-command setting, verified with make, clang, generation, and the
full race-detector test suite:

```sh
cd ~/dev/go-kubernetes-infrastructure-tutorial/lab
export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/go/bin:$PATH"
DEVELOPER_DIR=/Library/Developer/CommandLineTools make test
```

No global xcode-select setting, shell configuration, or license acceptance was
changed. make unit excludes the API-server integration tests; make test is the
complete current check. Tool and test-asset downloads stay under ignored lab/bin.

### 19 October 2026 - Conditions verification

Rechecked Homebrew Go: 1.27.2 darwin/arm64. The complete make test suite passed
with the integration tag and race detector after adding allocation conditions.
Reused the existing native 1.37.0 envtest assets and pinned tools, with the
per-command Command Line Tools setting above. No dependencies, persistent
services, cluster, kubeconfig, or global environment settings changed.
Temporal/Podman availability was not rechecked.

### 19 October 2026 - Manager/watch verification

The complete make test suite passed again with the race detector after adding
GPUCluster watch registration and configurable successful-check requeues
(default 30 seconds). One integration test now starts a real controller-runtime
manager against envtest, restricted to a separate test namespace. API creation,
status repair, and deletion/cleanup occur without manual Reconcile calls.
The test disables metrics/probes, cancels and joins the manager, then stops
the test API server. No standalone executable or persistent controller was
started; no kind cluster or kubeconfig changes. Existing dependency versions,
native test assets, and per-command toolchain settings were reused.

### 19 October 2026 - Temporal SDK verification

Added go.temporal.io/sdk v1.50.0 for a reservation-only workflow/activity and
in-process tests. Its Go 1.26 minimum is satisfied by the existing 1.27.2
toolchain. The SDK's module graph raises shared dependencies, including
golang.org/x/net v0.58.0, x/text v0.41.0, and grpc v1.83.2. The complete
make test suite passed with Kubernetes/envtest and the race detector after
these changes, using the same native assets and Command Line Tools setting.

The SDK suite skips retry delays and tests real activity retries against the
same in-memory provider. No Temporal service/worker was started or health
rechecked; these tests do not establish server compatibility, persisted-history
replay, or actual worker-restart recovery. No global settings, kubeconfig, or
Podman changes. make unit includes the SDK tests without an API server.

### Local service controls (last verified 17 October)

Temporal was running at the 17 October verification. After stopping it or rebooting, start it in a terminal following [Temporal's CLI documentation](https://docs.temporal.io/cli/setup-cli):

```sh
cd ~/dev/go-kubernetes-infrastructure-tutorial
mkdir -p .local/temporal
temporal server start-dev --db-filename .local/temporal/dev.db
```

The database file is configured to preserve history across service restarts. Stop a foreground instance with Ctrl-C. For the detached instance, read .local/temporal/server.pid, verify the PID still belongs to this Temporal command, then send it SIGTERM. Its PID file is not automatically refreshed if you start a new instance manually. A local Temporal service does not require a container engine through this route.

kubectl is installed using [the official macOS kubectl instructions](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/):

```sh
kubectl version --client
```

Choose a client within one Kubernetes minor version of the eventual cluster. Do not add the legacy Kubernetes 1.19 asset directory to PATH to fill this gap.

For envtest, the lab Makefile now pins versions and provides make envtest / make test to obtain matching native assets. See [Kubebuilder envtest documentation](https://book.kubebuilder.io/reference/envtest). The test plane includes the API server and etcd; it does not run the full cluster or built-in controllers. Do not point KUBEBUILDER_ASSETS at the old Intel binaries.

Podman was running at the 17 October verification. From a regular terminal, stop/start it with:

```sh
podman machine stop infrastructure-tutorial
podman machine start --update-connection infrastructure-tutorial
podman info
```

When starting through the agent's command runner, use the session job to preserve VM helpers. After a clean stop, restart its completed job with `launchctl kickstart gui/$(id -u)/local.infrastructure-tutorial.podman-start`. If it is no longer registered after logout/reboot, load the saved definition using `launchctl bootstrap gui/$(id -u) "$PWD/.local/podman/launch-agent.plist"` from the repo. Do not start another copy while the VM is already running.

Only if a full local integration exercise is useful (not yet executed):

```sh
KIND_EXPERIMENTAL_PROVIDER=podman kind create cluster --name infrastructure-tutorial
kubectl --context kind-infrastructure-tutorial get nodes
```

[kind documents Podman support](https://kind.sigs.k8s.io/docs/user/rootless/). The running VM and kind's cluster listing were verified, but full cluster creation has not been tested. Match the cluster image version to kubectl's supported minor-version range. Cluster creation downloads images and may update kubeconfig; use its dedicated context for subsequent commands.

## Shell resolution

The assistant's initial subprocess PATH omitted /opt/homebrew/bin and ~/go/bin. The user's .zprofile and .zshrc already include Homebrew shell setup, and .zshrc adds ~/go/bin. For an agent process that needs these tools:

```sh
export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/go/bin:$PATH"
```

No shell configuration was changed. A separate older Go installation exists at /usr/local/go/bin/go; prefer the Homebrew installation used in this audit.
