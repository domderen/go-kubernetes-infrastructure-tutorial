# Environment audit

Checked: 17 October 2026, Europe/Warsaw. This is a snapshot, not a promise that services remain running across sessions.

## Readiness

Ready to begin the tutorial's Go and fake-provider exercises. Further setup is needed for a live Temporal service and modern Kubernetes integration tooling. The core project uses simulated GPU hosts; it needs no physical GPUs, CUDA toolkit, NVIDIA drivers, Redfish hardware, or cloud account. A full kind cluster is optional.

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
| gopls | 0.19.1 at ~/go/bin/gopls | Binary runs; compatibility with the current Go version not tested. |
| Delve | 1.21.2 at ~/go/bin/dlv | Binary runs; actual debugging with Go 1.27 not tested. Optional for the tutorial. |
| Kubebuilder | 4.16.0 | Version command succeeded; project not scaffolded yet. |
| kind | 0.33.0 | Binary runs; cluster listing cannot complete while Podman is stopped. |
| Podman | Client 6.1.4; Podman Desktop installed | Existing podman-machine-default VM is stopped. Client cannot connect to its local service. |
| Helm | 4.3.1 | Version command succeeded; not required for the first exercises. |
| kubectl | No current command on PATH | Legacy /usr/local/kubebuilder/bin/kubectl runs as v1.19.2, darwin/amd64. Use a current native client for the future lab. |
| envtest assets | Legacy Intel binaries only in the standard legacy location | kube-apiserver 1.19.2 and etcd 3.4.9; unsuitable as the planned modern lab assets. No setup-envtest on checked PATH. Project-local assets are not provisioned yet. |
| Temporal CLI | Not found on checked PATH or common user/Homebrew executable locations | Needed for the proposed local service/history UI path; not needed to start Go or SDK-only workflow unit tests. |
| Go dependency access | proxy.golang.org reachable | HEAD request to Temporal SDK version list returned HTTP 200; project dependency resolution has not been exercised. |

Docker, Minikube, and Colima were not found on the checked PATH. They are not additional requirements for the proposed route: use the existing Podman installation if choosing kind, and use Temporal CLI for the local Temporal service.

Project libraries (Temporal SDK, controller-runtime, Kubernetes API packages, errgroup) should be added and pinned in lab/go.mod when introduced in a lesson. They are module dependencies, not globally installed applications. No module was initialized during setup, so module compatibility remains to be checked then.

## Verification evidence

An isolated temporary module containing a standard-library goroutine/channel test was run with:

```text
go test -race -count=1 ./...
```

The command succeeded with exit 0. This verifies compilation, C toolchain/race-runtime availability, and execution on this Mac. It does not implement or validate the tutorial's health checker. The temporary module was removed.

The existing Podman machine is configured with 4 CPUs, 8 GiB RAM, and a 70 GiB disk. `podman machine list` reported Running=false, and `podman info` / `kind get clusters` failed to connect to its local socket. A running cluster was not verified.

## Setup commands for later lessons (not executed)

For the local Temporal service, [Temporal's Go setup documentation](https://docs.temporal.io/develop/go/set-up-your-local-go) recommends the CLI:

```sh
brew install temporal
cd ~/dev/go-kubernetes-infrastructure-tutorial
mkdir -p .local/temporal
temporal server start-dev --db-filename .local/temporal/dev.db
```

The database file preserves history across service restarts. Stop the foreground service with Ctrl-C. The default service and UI addresses are localhost:7233 and http://localhost:8233. Check CLI help at execution time if flags have changed. A local Temporal service does not require a container engine through this route.

For a modern Kubernetes CLI, follow [the official macOS kubectl instructions](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/):

```sh
brew install kubectl
kubectl version --client
```

Choose a client within one Kubernetes minor version of the eventual cluster. Do not add the legacy Kubernetes 1.19 asset directory to PATH to fill this gap.

For envtest, choose versions with the controller module, then use the generated Makefile's `make envtest` / `make test` targets to obtain matching native assets. See [Kubebuilder envtest documentation](https://book.kubebuilder.io/reference/envtest). The test plane includes the API server and etcd; it does not run the full cluster or built-in controllers. Do not point KUBEBUILDER_ASSETS at the old Intel binaries.

Only if a full local integration exercise is useful:

```sh
podman machine start podman-machine-default
podman info
KIND_EXPERIMENTAL_PROVIDER=podman kind create cluster --name infrastructure-tutorial
kubectl --context kind-infrastructure-tutorial get nodes
```

[kind documents Podman support](https://kind.sigs.k8s.io/docs/user/rootless/). This Mac's Podman/kind combination has not yet been tested with a running VM; do not mark cluster readiness until the node check succeeds. Cluster creation downloads images and may update kubeconfig; use its dedicated context for subsequent commands.

## Shell resolution

The assistant's initial subprocess PATH omitted /opt/homebrew/bin and ~/go/bin. The user's .zprofile and .zshrc already include Homebrew shell setup, and .zshrc adds ~/go/bin. For an agent process that needs these tools:

```sh
export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/go/bin:$PATH"
```

No shell configuration was changed. A separate older Go installation exists at /usr/local/go/bin/go; prefer the Homebrew installation used in this audit.
