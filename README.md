# Codex Session Operator

Codex Session Operator runs coding tasks against repositories through Kubernetes. You declare reusable Agents, execution Environments, and Profiles as custom resources. The gateway accepts a task from a Profile, stores it durably, and creates one Session for that conversation.

## Architecture

The controller and gateway are separate services with separate container images. They are installed together because a conversation needs both: the gateway owns durable input and history, while the controller owns Kubernetes and OpenAI resource lifecycles.

![Session flow: install separate controller and gateway Deployments together, then start a Profile-backed Session with durable history and an application executor](docs/assets/session-flow.gif)

The system separates these responsibilities:

| Component | Responsibility |
| --- | --- |
| Controller Deployment | Reconciles CRDs, verifies ownership, creates remote resources, and manages executor Pods and workspaces. |
| Gateway Deployment | Authenticates callers, accepts tasks, creates Sessions from Profiles, delivers messages, and saves history and approvals. |
| PostgreSQL | Stores gateway data. Provision it separately as a managed service such as Amazon RDS, or as a local development database. |
| Codex executor image | Provides `codex exec-server`, Git, repository preparation, and basic runtime tools. |
| Application executor image | Extends the Codex executor with project tools. The Go example adds Go, Make, a C compiler, and GitHub CLI. |

On first startup, gateway replicas serialize initialization from the embedded `schema.sql` with a PostgreSQL advisory lock. The database role must own the target schema and have table creation and data access privileges. Later starts keep the existing data.

## Custom resources

All resources use `agents.softcane.io/v1alpha1` and belong to one Kubernetes namespace.

| Resource | Purpose |
| --- | --- |
| `Agent` | Model, instructions, reasoning settings, and optional tools. |
| `Environment` | Execution recipe: no runtime, OpenAI-hosted runtime, or a self-hosted executor. |
| `Profile` | Reusable binding of an Agent, Environment, optional Vaults, and an explicit spending policy. |
| `Session` | One conversation attempt. The gateway creates it when a caller starts a task from a Profile. |
| `Vault` and `VaultCredential` | References to credentials for connected provider tools. |

Applying an Agent, Environment, and Profile does not create a task Pod. A self-hosted executor Pod and its workspace are created only after the gateway accepts a task and creates a Session.

## `my-go-project` example

The included [my-go-project](examples/my-go-project/) is a small Go service with an idempotency bug. Its application executor contains the Go toolchain and test dependencies; its skills remain in a separate immutable capability image.

The following declarations show the real API shape. Replace the image digests, repository commit, architecture, and StorageClass with values approved by your installation.

```yaml
apiVersion: agents.softcane.io/v1alpha1
kind: Agent
metadata:
  name: my-go-maintainer
  namespace: codex-session-operator-system
spec:
  model: gpt-6-luna
  instructions: |
    Maintain my-go-project using the requested task mode.
    Read /workspace/AGENTS.md and docs/architecture.md first.
    Registered project skills live under /capabilities/my-go-project/skills.
    Run make bootstrap, reproduce failures, make the requested repair,
    and run make verify. Report the commands and results.
  reasoning:
    effort: high
  multiAgent:
    enabled: false
  deletionPolicy: Delete
---
apiVersion: agents.softcane.io/v1alpha1
kind: Environment
metadata:
  name: my-go-development
  namespace: codex-session-operator-system
spec:
  mode: selfHosted
  deletionPolicy: Delete
  selfHosted:
    image: registry.example.com/codex/my-go-executor@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    architecture: amd64
    workspaceDirectory: /workspace
    workspaceSize: 4Gi
    workspaceStorage:
      type: Persistent
      storageClassName: approved-storage-class
      claimDeletionPolicy: Retain
    repository:
      url: https://github.com/example/my-go-project.git
      commit: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
      credentialSecretRef:
        name: codex-repository
        key: token
    resources:
      cpuRequest: 500m
      cpuLimit: "2"
      memoryRequest: 512Mi
      memoryLimit: 2Gi
    capabilities:
      - name: my-go-project
        image: registry.example.com/codex/my-go-skills@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
        path: /capabilities/my-go-project
        type: skills
---
apiVersion: agents.softcane.io/v1alpha1
kind: Profile
metadata:
  name: my-go-project
  namespace: codex-session-operator-system
spec:
  agentRef:
    name: my-go-maintainer
  environmentRef:
    name: my-go-development
  spendControl:
    mode: unlimited
  vaultRefs: []
```

The Profile becomes Ready when its referenced policy resources are Ready. Readiness does not prove that repository checkout, the executor, or a model task has run; those exist only for a Session.

Follow [Run `my-go-project`](docs/my-go-project.md) for the complete image, credential, installation, task, recovery, and cleanup walkthrough. The source templates are in [examples/my-go-project/automation/k8s](examples/my-go-project/automation/k8s/).

## Task and state lifecycle

1. A client sends a Profile name, message, and idempotency key to the gateway.
2. The gateway commits the request and receipt to PostgreSQL, then returns HTTP `202` with a conversation URL.
3. A gateway worker creates a Session from the frozen Profile selection.
4. The controller resolves the selected resources and creates the remote Session. For a self-hosted Environment it also creates the executor Pod and workspace.
5. The gateway saves provider turns and history. The client reads the conversation to observe completion, send more input, or close it.

HTTP `202` means the input was accepted durably. It does not mean the executor ran or the turn completed. Inspect a saved root turn and its output to establish the result.

| Store | State it owns |
| --- | --- |
| Kubernetes | Desired configuration, remote IDs, conditions, safe dependency snapshots, creation claims, and finalizers. |
| Gateway PostgreSQL | Messages, receipts, replay identities, pending delivery, saved turns, and approvals. |
| Workspace PVC | Repository files, edits, local commits, and task artifacts for one self-hosted Session. |
| OpenAI | Managed Agent and Session identities, reasoning, and remote turns. |

Conversation content and credential values do not belong in CR status. A retained workspace does not replace PostgreSQL history, and a new Session does not automatically adopt a retained PVC.

## Install and operate

Use the [deployment guide](docs/deployment.md) to build the separate images, provision PostgreSQL and credentials, and apply the combined controller and gateway installation. PostgreSQL lifecycle, backups, high availability, and restore testing remain installation responsibilities.

See [resources](docs/resources.md) for all six CRDs, execution modes and Vault credentials; [architecture](docs/architecture.md) for component boundaries and recovery; [conversations](docs/conversations.md) for the HTTP and retry contract; and [development](docs/development.md) for repository checks. The generated [CRD schemas](config/crd/bases/) define the accepted fields for this checkout.

## License

[Apache License 2.0](LICENSE).
