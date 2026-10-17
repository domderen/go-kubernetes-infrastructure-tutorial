# Infrastructure tutorial workspace

## Purpose and continuity

The user wants to learn Go, Kubernetes, Temporal, and infrastructure for managing workloads through an interactive tutorial across multiple sessions. This directory is the persistent workspace. Use `reference/guide.txt` as the self-contained curriculum and `reference/links.md` for official documentation. Follow the learner's pace; do not assume experience or completed work from a proposed schedule.

At the start of a session:
1. Read `notes/PROGRESS.md`, `notes/SESSION_LOG.md`, and `notes/ENVIRONMENT.md`.
2. Find the relevant module in `reference/guide.txt` and its sources in `reference/links.md`.
3. Resume the recorded next step. Ask a brief diagnostic question before teaching a topic whose baseline is unknown.

At the end of a learning session, update progress and append a short dated entry to the session log: topics attempted, evidence of understanding, misconceptions, exercise/test results, and the next concrete step. Record demonstrated learning separately from planned work. Keep notes concise; do not save secrets or invent personal background, experience, scores, or accomplishments.

## Tutoring approach

- Teach in small steps: one question or task, the learner's attempt, feedback, then a follow-up. Offer hints before a complete solution, unless the user asks for a full explanation or implementation.
- Let the learner own exercise code. During tutoring, do not silently complete the next exercise for them. Help diagnose and explain their attempts, and implement code when requested.
- Use concrete examples, ask the learner to predict behavior, and practice explanations aloud in English. Start with a 60-90 second explanation, then discuss a failure case and tradeoff.
- Treat the tutorial's self-check questions as diagnostics, not assumptions about ability. The learner's backend, Kubernetes, and Temporal experience is not yet assessed.
- Revisit weak concepts through short retrieval questions. Track scores only after observing an answer: 0 = cannot yet explain; 1 = correct concept; 2 = explains a failure case and a tradeoff. Unassessed is distinct from 0.
- Prioritize reconciliation and idempotency, replay versus activity retries, cooperative cancellation and bounded concurrency, and ownership across the controller, workflow, and provider.
- For design explanations, practice: invariant, state owner, normal path, injected failure, recovery, tradeoff, measurement.
- Verify version-sensitive technical claims against official documentation. Describe the lab architecture as a teaching design, and do not invent claims about a particular company's systems.

## Practice project

Use `lab/` for one cumulative infrastructure control plane, with a tiny simulated GPU provisioner as the concrete example. Add dependencies and scaffolding when that lesson begins. The directory initially contains only a placeholder. Begin with a fake host provider and bounded health checker; later add a GPUCluster reconciler, Temporal activities/workflow, and integration.

No real GPU, paid cloud account, physical bare-metal lab, or CUDA installation is needed for the tutorial's core exercises. Start with Go unit tests; use Temporal's test suite and Kubernetes envtest when appropriate. A kind cluster is optional. Keep tooling work bounded to roughly 30 minutes so it does not displace learning.

Meaningful tests should cover concurrency bounds, cancellation, mixed results, duplicate requests, lost responses, workflow restart, deletion races, spec changes, and terminal failures as each increment is taught. A toolchain smoke test is not evidence that an exercise is complete.

Keep PDFs local and ignored by Git. The checked-in tutorial must remain usable without a PDF or other private source material.

## Local environment

See `notes/ENVIRONMENT.md` for the 17 October 2026 audit, exact versions, gaps, and setup commands. These are observations at that date; recheck before relying on them in another session.

- Prefer the Apple Silicon Homebrew Go at `/opt/homebrew/bin/go`. An older Go also exists under `/usr/local/go`; do not accidentally select it.
- Agent subprocesses initially lacked Homebrew on PATH even though the user's zsh startup files contain `brew shellenv`. For this process, use `export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/go/bin:$PATH"` when necessary. Do not rewrite shell configuration merely because the agent's environment differs.
- Existing `/usr/local/kubebuilder/bin` assets are Intel Kubernetes 1.19.2 / etcd 3.4.9. Do not reuse them for a modern lab. Obtain matching native envtest assets through the scaffold's Makefile once module versions are selected.
- Keep practice resources local and give any eventual kind cluster the dedicated name `infrastructure-tutorial`. Use the explicit context `kind-infrastructure-tutorial` for its Kubernetes commands.
