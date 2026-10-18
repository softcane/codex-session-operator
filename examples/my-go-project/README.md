# my-go-project

This small Go HTTP job service is the self-hosted repository example for Codex Session Operator. Its initial revision contains an idempotency bug: two submissions with the same request ID allocate two jobs. The walkthrough asks an agent to reproduce the bug, add a failing regression, repair it, and verify the result.

Run `make bootstrap` once, then use `make verify` for the project checks. Start the service with `go run ./cmd/service`; its `POST /jobs` endpoint accepts `{"request_id":"one"}`.

The example keeps each concern in a separate artifact:

| Path | Responsibility |
| --- | --- |
| `AGENTS.md` | Repository rules, commands, and verification expectations. |
| `.agents/skills/` | Project procedures used by the Agent. |
| `sandbox/Dockerfile.executor` | Application executor layered on the shared Codex executor; it adds Go, Make, a C compiler, and GitHub CLI. |
| `sandbox/Dockerfile.skills` | Immutable skills bundle registered through the Environment. |
| `automation/k8s/` | Agent, Environment, and Profile templates. |

Applying those three custom resources declares a reusable recipe. It creates no Session, executor Pod, or workspace. The gateway creates those resources only after it durably accepts a task from the Profile.

See the complete [my-go-project walkthrough](../../docs/my-go-project.md) for image builds, installation, task submission, recovery, and cleanup.
