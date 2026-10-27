# my-go-project

This small Go HTTP job service is the self-hosted repository example for Codex Session Operator. Its initial revision contains an idempotency bug: two submissions with the same request ID allocate two jobs. Use it as a bug-fix task: ask an agent to reproduce the bug, add a failing regression, repair it and verify the result.

Run `make bootstrap` once, then use `make verify` for the project checks. Start the service with `go run ./cmd/service`; its `POST /jobs` endpoint accepts `{"request_id":"one"}`.

The example keeps each concern in a separate artifact:

| Path | Responsibility |
| --- | --- |
| `AGENTS.md` | Repository rules, commands, and verification expectations. |
| `.agents/skills/` | Project procedures used by the Agent. |
| `sandbox/Dockerfile.executor` | Application executor layered on the shared Codex executor; it adds Go, Make, a C compiler, and GitHub CLI. |
| `sandbox/Dockerfile.skills` | Immutable skills bundle registered through the Environment. |
| `automation/k8s/` | Agent, Environment and Profile declarations, plus the administrator SandboxTemplate and pool. |

Administrators install the SandboxTemplate and its zero-spare pool. The Environment selects that pool; the Profile selects the Environment and Agent. After accepting a task, the gateway creates a Session and our controller creates its SandboxClaim. Agent Sandbox creates and manages the Sandbox, executor Pod and workspace PVC.

Start with [getting started](../../docs/getting-started.md) to run a public repository task. Use [deployment](../../docs/deployment.md) to adapt this example's executor, skills image and Kubernetes templates for your own installation.
