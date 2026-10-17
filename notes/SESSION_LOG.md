# Session log

## 17 October 2026 - Workspace and machine audit

Established a persistent tutorial workspace with a curriculum, official documentation links, tutoring instructions, progress tracker, session notes, and an environment audit. Reserved lab/ without implementing the learner's exercises.

Machine: Apple Silicon macOS 26.6, 32 GiB RAM, 10 logical CPUs, approximately 135 GiB available disk at audit time. Confirmed Homebrew Go 1.27.2 compiles and runs an isolated standard-library test under the race detector, exit 0. No Go modules were downloaded for that check; the temporary test was removed. Confirmed Git, C compiler, Make, VS Code with the Go extension, kind, Kubebuilder, Helm, and Podman client installations. Go module proxy responded with HTTP 200.

Gaps: Temporal CLI absent from checked tool locations; no current kubectl on PATH; legacy Kubernetes 1.19.2 Intel envtest assets exist; existing Podman VM is stopped, so no running kind environment was verified. gopls and Delve are installed but older, and editor/debugger compatibility with Go 1.27 has not been tested.

No packages installed, services started, clusters created, global shell settings changed, or existing Kubernetes contexts modified. No learner ability assessed and no curriculum exercises completed.

Next step: short baseline check, then Module 1 Go concurrency/cancellation and the host-health checker.

## 17 October 2026 - Tutorial documentation

Organized the material as a self-contained Go and Kubernetes infrastructure tutorial, with Temporal for durable operations and a simulated host provider for local exercises. Added a case-insensitive PDF ignore rule. Learning progress remains unassessed; the lab has no exercise implementation.
