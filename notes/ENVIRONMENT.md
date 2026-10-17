# Environment audit

Checked: 17 October 2026, Europe/Warsaw. This is a snapshot, not a promise that services remain running across sessions.

## Readiness

Ready to begin the Go exercises. Temporal CLI and modern kubectl are installed, Go editor/debugger tools are updated, and local Temporal and Podman services are running as verified below. Project dependencies and matching native envtest assets will be selected when their lessons begin. The core project uses simulated GPU hosts; it needs no physical GPUs, CUDA toolkit, NVIDIA drivers, Redfish hardware, or cloud account. A full kind cluster is optional and has not been created.

### Before installation, 17 October 2026 (historical)

The earlier audit found Temporal CLI and modern kubectl missing, older gopls/Delve versions, and only legacy Intel envtest binaries. The follow-up initially could not inspect Podman's lockfile inside the sandbox. Installation and service setup were subsequently explicitly authorized and performed with elevated access. The table below reflects the resulting state; lab/ still contains only its placeholder.

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
| envtest assets | Legacy Intel binaries only in the standard legacy location | kube-apiserver 1.19.2 and etcd 3.4.9; unsuitable as the planned modern lab assets. No setup-envtest on checked PATH. Project-local assets are not provisioned yet. |
| Temporal CLI | Installed 1.9.1 at /opt/homebrew/bin/temporal; embedded server 1.32.0, UI 2.54.1 | Native arm64; local service health SERVING, default namespace registered, web UI HTTP 200. |
| Go dependency access | proxy.golang.org reachable | HEAD request to Temporal SDK version list returned HTTP 200; project dependency resolution has not been exercised. |

Docker, Minikube, and Colima were not found on the checked PATH. They are not additional requirements for the proposed route: use the existing Podman installation if choosing kind, and use Temporal CLI for the local Temporal service.

Project libraries (Temporal SDK, controller-runtime, Kubernetes API packages, errgroup) should be added and pinned in lab/go.mod when introduced in a lesson. They are module dependencies, not globally installed applications. No module was initialized during setup, so module compatibility remains to be checked then.

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

Temporal is already running. After stopping it or rebooting, start it in a terminal following [Temporal's CLI documentation](https://docs.temporal.io/cli/setup-cli):

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

For envtest, choose versions with the controller module, then use the generated Makefile's `make envtest` / `make test` targets to obtain matching native assets. See [Kubebuilder envtest documentation](https://book.kubebuilder.io/reference/envtest). The test plane includes the API server and etcd; it does not run the full cluster or built-in controllers. Do not point KUBEBUILDER_ASSETS at the old Intel binaries.

Podman is already running. From a regular terminal, stop/start it with:

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
