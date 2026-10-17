# Session log

## 17 October 2026 - Workspace and machine audit

Established a persistent tutorial workspace with a curriculum, official documentation links, tutoring instructions, progress tracker, session notes, and an environment audit. Reserved lab/ without implementing the learner's exercises.

Machine: Apple Silicon macOS 26.6, 32 GiB RAM, 10 logical CPUs, approximately 135 GiB available disk at audit time. Confirmed Homebrew Go 1.27.2 compiles and runs an isolated standard-library test under the race detector, exit 0. No Go modules were downloaded for that check; the temporary test was removed. Confirmed Git, C compiler, Make, VS Code with the Go extension, kind, Kubebuilder, Helm, and Podman client installations. Go module proxy responded with HTTP 200.

Gaps: Temporal CLI absent from checked tool locations; no current kubectl on PATH; legacy Kubernetes 1.19.2 Intel envtest assets exist; existing Podman VM is stopped, so no running kind environment was verified. gopls and Delve are installed but older, and editor/debugger compatibility with Go 1.27 has not been tested.

No packages installed, services started, clusters created, global shell settings changed, or existing Kubernetes contexts modified. No learner ability assessed and no curriculum exercises completed.

Next step: short baseline check, then Module 1 Go concurrency/cancellation and the host-health checker.

## 17 October 2026 - Tutorial documentation

Organized the material as a self-contained Go and Kubernetes infrastructure tutorial, with Temporal for durable operations and a simulated host provider for local exercises. Added a case-insensitive PDF ignore rule. Learning progress remains unassessed; the lab has no exercise implementation.

## 17 October 2026 - Environment preparation review

Rechecked installed tool versions and confirmed missing Temporal CLI, modern kubectl, and project-local envtest setup. Verified installation guidance against official documentation. Recommended installing the two CLIs, refreshing gopls and optionally Delve, and deferring native envtest assets and pinned libraries to scaffolding. Podman VM state could not be rechecked because sandbox access to its lockfile was denied; the previous stopped state is historical. No tools installed or services started. No learning assessment or exercise attempted. Next step: address environment preparation, then the short Go baseline check.

## 17 October 2026 - Tools and local services prepared

At the learner's request, installed Temporal CLI 1.9.1 (server 1.32.0, UI 2.54.1) and kubectl 1.37.1; updated gopls to 0.24.0 and Delve to 1.27.2. All four binaries are native arm64. Started Temporal on loopback with its database, log, and PID file under ignored .local/temporal; health returned SERVING, default namespace is registered, and UI returned HTTP 200.

Removed podman-machine-default and its VM storage with explicit authorization. Created infrastructure-tutorial using applehv, 4 CPUs, 8 GiB RAM, 70 GiB disk, rootful containers inside the VM. Command-session cleanup initially killed helpers and interrupted initialization; recreated the disk and used a session-only macOS job with AbandonProcessGroup to preserve helpers. Confirmed running state, matching Podman client/server 6.1.4, and successful arm64 hello-world execution. No tutorial exercise or learner assessment completed. Next step: short experience/time baseline, then the first Go self-check and CheckHosts increment.
