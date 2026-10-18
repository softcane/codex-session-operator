# Resource reference

Codex Session Operator exposes six namespaced resources in `agents.softcane.io/v1alpha1`. The generated [CRD schemas](../config/crd/bases/) define accepted fields and validation for this checkout.

## Resource relationships

| Resource | Role | Creates task compute? |
| --- | --- | --- |
| `Agent` | Reusable model, instructions, reasoning and tools backed by an OpenAI Agent. | No. |
| `Environment` | Reusable execution recipe: `none`, `openaiHosted` or `selfHosted`. | No. |
| `Profile` | Gateway-approved binding of one Agent, one Environment, optional Vaults and spend control. | No. |
| `Vault` | Reusable remote credential container. | No. |
| `VaultCredential` | Credential metadata and a protected same-namespace Secret reference. | No. |
| `Session` | One remote conversation with frozen dependency identities. | A self-hosted Session creates an executor Pod and optional PVC. |

Agent instructions are reusable configuration. Task messages, turns, approvals and replay identities belong in PostgreSQL. Resource status contains remote IDs, conditions and safe dependency snapshots, without conversation text or credential values.

## Agent

An Agent declares the model and reusable behavior. Its `tools` field replaces the complete remote tool list when present, including an explicit empty list. Session overrides can change supported Agent settings for one Session.

The [README example](../README.md#my-go-project-example) shows a repository-maintenance Agent. See the [Agent API source](../api/v1alpha1/agent_types.go) and generated schema for the full reasoning, output, multi-agent and tool variants.

## Environment

| Mode | Execution | Per-Session Kubernetes resources |
| --- | --- | --- |
| `none` | OpenAI reasoning and permitted service tools, without an execution filesystem. | Session CR only. The first durable message accompanies remote creation. |
| `openaiHosted` | OpenAI-managed runtime created from the Environment template. | Session CR; OpenAI reports runtime readiness. |
| `selfHosted` | OpenAI runs the agent while `codex exec-server` supplies commands and files from your cluster. | Session CR, executor Pod and optional workspace PVC. |

A text-only Environment has no mode-specific branch:

```yaml
apiVersion: agents.softcane.io/v1alpha1
kind: Environment
metadata:
  name: text-only
  namespace: codex-session-operator-system
spec:
  mode: none
  deletionPolicy: Delete
```

An OpenAI-hosted Environment supplies its complete hosted template:

```yaml
apiVersion: agents.softcane.io/v1alpha1
kind: Environment
metadata:
  name: hosted-small
  namespace: codex-session-operator-system
spec:
  mode: openaiHosted
  deletionPolicy: Delete
  openaiHosted:
    name: hosted-small
    containerSize: small
    network:
      access: disabled
```

A self-hosted Environment selects an approved immutable application executor image, architecture, workspace policy and optional repository or capability bundles. See the [persistent Environment sample](../config/samples/agents_v1alpha1_environment_persistent.yaml) and [my-go-project Environment](../examples/my-go-project/automation/k8s/environment.yaml.tmpl).

Environment readiness validates the recipe. It does not prove that repository preparation, an executor connection or a model task has run. Those checks happen for each Session.

## Profile

A Profile is the gateway entry point for a new conversation. It contains same-namespace references and spend policy, without prompts, instructions or credentials.

```yaml
apiVersion: agents.softcane.io/v1alpha1
kind: Profile
metadata:
  name: investigation
  namespace: codex-session-operator-system
spec:
  agentRef:
    name: investigator
  environmentRef:
    name: text-only
  spendControl:
    mode: limited
    limitCents: 500
  vaultRefs: []
```

Profile readiness means its selected policy resources are Ready. Starting a task through the conversation API freezes the Profile identity, policy and selected dependency identities before the gateway creates a Session.

## Direct Session

Use a direct Session when the caller must choose already-approved resource references without creating a reusable Profile. A new Session requires same-namespace `agentRef` and `environmentRef` plus explicit `spendControl`.

```yaml
apiVersion: agents.softcane.io/v1alpha1
kind: Session
metadata:
  name: manual-investigation
  namespace: codex-session-operator-system
spec:
  agentRef:
    name: investigator
  environmentRef:
    name: text-only
  spendControl:
    mode: limited
    limitCents: 500
  vaultRefs: []
```

The authenticated gateway can create the same resource through `POST /v1/namespaces/{namespace}/sessions`:

```json
{
  "name": "manual-investigation",
  "spec": {
    "agentRef": {"name": "investigator"},
    "environmentRef": {"name": "text-only"},
    "spendControl": {"mode": "limited", "limitCents": 500},
    "vaultRefs": []
  }
}
```

The route returns `201` with the Session URL. A `none` Environment waits for durable initial input. Send it to `POST /v1/namespaces/{namespace}/sessions/{name}/messages` with an idempotency key, then read `/history` for the saved result.

These live Session routes remain available while the Session exists:

| Route | Purpose |
| --- | --- |
| `GET /sessions/{name}` | Read gateway delivery state. |
| `POST /sessions/{name}/messages` | Submit durable input. |
| `GET /sessions/{name}/history` | Read saved provider turns and items. |
| `GET /sessions/{name}/events` | Stream live Session events. |
| `POST /sessions/{name}/cancel` | Cancel the selected active turn. |
| `/sessions/{name}/actions/...` | Read or resolve pending tool approvals. |

Use the full `/v1/namespaces/{namespace}` prefix for every route. Conversation URLs remain the preferred interface for Profile-backed tasks because they stay readable after Session cleanup.

## Vault credentials and MCP tools

Vault credential values come from the fixed same-namespace `codex-vault` Secret. The CR records the protected source identity and provider credential ID, without copying the secret value into status.

This example configures one service-origin HTTP MCP tool and a matching bearer credential:

```yaml
apiVersion: agents.softcane.io/v1alpha1
kind: Agent
metadata:
  name: service-agent
  namespace: codex-session-operator-system
spec:
  model: gpt-6-luna
  instructions: Use the approved service when the task requires it.
  tools:
    - type: mcp
      mcp:
        serverLabel: service
        transport:
          type: http
          http:
            serverURL: https://mcp.example.com/mcp
        connectionOrigin: service
        required: true
  deletionPolicy: Delete
---
apiVersion: agents.softcane.io/v1alpha1
kind: Vault
metadata:
  name: service-credentials
  namespace: codex-session-operator-system
spec:
  deletionPolicy: Delete
---
apiVersion: agents.softcane.io/v1alpha1
kind: VaultCredential
metadata:
  name: service-token
  namespace: codex-session-operator-system
spec:
  vaultRef:
    name: service-credentials
  name: Service MCP bearer credential
  auth:
    type: static_bearer
    staticBearer:
      serverURL: https://mcp.example.com/mcp
      tokenSecretRef:
        name: codex-vault
        key: mcp-token
  deletionPolicy: Delete
---
apiVersion: agents.softcane.io/v1alpha1
kind: Profile
metadata:
  name: service-tasks
  namespace: codex-session-operator-system
spec:
  agentRef:
    name: service-agent
  environmentRef:
    name: text-only
  spendControl:
    mode: limited
    limitCents: 500
  vaultRefs:
    - name: service-credentials
```

Create the `codex-vault` Secret separately. When one attached credential matches the MCP URL, the controller selects it. If several attached credentials match, set `Agent.spec.tools[].mcp.credentialID` to the chosen provider ID from `VaultCredential.status.openaiCredentialID`.

`static_bearer` and `mcp_oauth` credentials apply to service-origin HTTP MCP tools. `environment_variable` credentials supply placeholders to supported hosted environments; they do not inject values into self-hosted executor Pods. The executor connection key in `codex-executor` has a separate purpose.

## Deletion and ownership

Reusable Agent, Environment, Vault and VaultCredential resources use explicit `Delete` or `Retain` policies. Profile deletion removes only the reusable binding. A Session always cleans up its owned remote conversation and, when self-hosted, its executor before its finalizer completes.

For persistent self-hosted workspaces, `claimDeletionPolicy` controls whether Session cleanup deletes or retains the PVC. A retained claim does not attach to a later Session. The StorageClass volume reclaim policy and backup policy are separate controls.

The controller keeps creation claims and finalizers when a remote create or delete outcome is uncertain. Recover the exact object through its Kubernetes UID and verified owner scope; do not clear claims or finalizers to force progress.

## Related documentation

- [Architecture](architecture.md) explains lifecycle and state ownership.
- [Conversations](conversations.md) defines Profile-backed task, retry and completion semantics.
- [Deployment](deployment.md) covers images, Secrets, PostgreSQL and executor policy.
- [CRD schemas](../config/crd/bases/) remain the field-level contract.
