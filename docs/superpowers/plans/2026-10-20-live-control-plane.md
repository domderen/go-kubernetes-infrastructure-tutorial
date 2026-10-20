# Live control plane implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development for independent implementation tasks and a whole-change review. Preserve existing work; do not commit or push.

**Goal:** Run the complete simulated GPUCluster lifecycle against real local Kubernetes and Temporal servers.

**Architecture:** UID-owned persistent HTTP mock provider; Temporal owns provisioning; Kubernetes owns desired state, workflow identity, status, and deletion. Host binaries connect to the dedicated kind cluster and local Temporal dev server.

**Tech Stack:** Existing pinned Go/Temporal/controller-runtime libraries, standard net/http and JSON atomic persistence, kind on Podman.

**Spec:** ../specs/2026-10-20-live-control-plane.md

## Global constraints

- Keep resources local; kind name infrastructure-tutorial, context kind-infrastructure-tutorial, kubeconfig .local/kubeconfig.
- Preserve all current source/notes changes. Existing main checkout continues the authorized tutorial work; no extra worktree, commit, push, cloud or real GPU requirements.
- Use Homebrew Go and per-command DEVELOPER_DIR=/Library/Developer/CommandLineTools.
- Provider records and Temporal history survive their process restarts. One provider owns its JSON file.

## Review focus

- Lost HTTP response after persisted creation must recover the same logical job.
- Controller status loss must never start a new completed workflow.
- Deletion while a worker is absent must fence late work before finalizer removal.
- One failing parallel host must cancel/join remaining workflow work before cleanup.
- Startup scripts must target only this lab's cluster/processes and preserve state.

## Tasks

### 1. Persistent mock provider (mockprovider agent)

Files: lab/mockprovider/*.go. Contracts are fixed in the spec and lab/lifecycle.go.
- [x] Write failing tests for persisted duplicate allocation/image recovery, closed UID, response loss, join/validation preconditions, and HTTP error/context behavior.
- [x] Implement atomic state commits, JSON HTTP API/client, delayed simulated completion, fault injection, and inspection.
- [x] Run focused race tests; report public routes and evidence. No unrelated file edits.

### 2. Complete Temporal lifecycle (workflow agent)

Files: lab/provisioning/*.go and provisioning/README.md. Consume the shared HostOperation interface; do not depend on mockprovider concrete types.
- [x] Write failing SDK tests for bounded per-host image/join/validate, stable retry identity, permanent validation failure, failure/cancellation cleanup, and preserving successful progress.
- [x] Add lifecycle activities and bounded orchestration; retain existing reservation/image tests, adapting successful fixtures to register all activities.
- [x] Run focused race tests and report options/registration requirements. Keep provider interfaces at consumers.

### 3. Integrated controller (controller agent)

Files: lab/controller/provisioning*.go, lab/api/v1alpha1/gpucluster_types.go. Preserve the allocation-only adapter.
- [x] Write failing tests for finalizer-first dispatch, stable UID/generation workflow identity, completed workflow recovery after status loss, failed conditions, spec defaults, and fenced deletion ordering.
- [x] Implement Temporal adapter and integrated reconciler, API status/spec fields; generate API/CRD through existing Makefile.
- [x] Run focused controller/envtest tests, retaining all old lifecycle tests.

### 4. Runtime and live verification (root)

Files: lab/cmd/{provider,worker,controller,replay}/main.go, lab/scripts/*, config samples, Makefile and live README.
- [x] Verify/start Temporal and create the dedicated kind cluster without disturbing unrelated kubeconfig.
- [x] Wire host binaries, process controls, loopback provider endpoint, explicit kubeconfig/context, sample resource and real Deployment.
- [x] Run complete race/envtest tests, build binaries, and execute live success/failure/restart/deletion/spec/history scenarios. Save local evidence and a persistent Ready sample.
- [x] Obtain independent review, resolve findings, rerun affected verification, update notes and reproducible documentation.

## Ledger

Ruling: execute the explicitly requested full implementation without additional design approval gates; the user asked to build and test all of it. The design fixes local teaching choices; adjustments remain reversible.

Ruling: independent source ownership permits provider, workflow, and controller implementation in parallel; root owns binaries, scripts, cluster and final integration. No agent may edit outside its assigned files, commit, push, or start external resources.

Completed 20 October: all four deliverables verified; make test and12 live checks passed. Independent review findings resolved. Live stopped-worker deletion exposed generation increment selecting a wrong workflow; metadata workflow identity now persists before dispatch and survives status wipes/deletion. Actual history confirms cancellation acknowledgment before compensation. Ready demo/services left running. No Git commit/push performed.
