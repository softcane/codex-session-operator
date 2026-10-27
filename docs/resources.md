# Resource reference

Codex Session Operator exposes six namespaced resources in `agents.softcane.io/v1alpha1`. Use this page to look up resource roles, common configuration, update rules and cleanup behavior. The generated [CRD schemas](../config/crd/bases/) define every accepted field and its validation for this checkout.

These examples use the configured provider. Real OpenAI execution incurs charges. Start with [getting started](getting-started.md) for a complete local setup.

Enum values are case-sensitive. Operator-owned modes use `SelfHosted`, `OpenAIHosted`, `None`, `Limited` and `Unlimited`; hosted source selectors use `FileID`, `Inline` and `SkillReference`; deletion policies use `Delete` and `Retain`. Provider/protocol values such as `reasoning.effort: high`, tool types, hosted network access and OAuth methods retain their documented wire literals. The adapter translates CRD field names, including `workingDirectory` to `cwd` and `expirationTime` to `expires_at`.

## Resource relationships

| Resource | Role | Creates task compute? |
| --- | --- | --- |
| `Agent` | Reusable model, instructions, reasoning and tools backed by an OpenAI Agent. | No. |
| `Environment` | Reusable execution recipe: `None`, `OpenAIHosted` or `SelfHosted`. | No. |
| `Profile` | Gateway-approved binding of one Agent, one Environment, optional Vaults and spend control. | No. |
| `Vault` | Reusable remote credential container. | No. |
| `VaultCredential` | Credential metadata and a protected same-namespace Secret reference. | No. |
| `Session` | One remote conversation with frozen dependency identities. | A self-hosted Session creates a SandboxClaim; Agent Sandbox creates compute and storage. |

Agent instructions are reusable configuration. Task messages, turns, approvals and replay identities belong in PostgreSQL. Resource status contains remote IDs, conditions and safe dependency snapshots, without conversation text or credential values.

## Agent

An Agent declares the model and reusable behavior. Its `tools` field replaces the complete remote tool list when present, including an explicit empty list. Session overrides can change supported Agent settings for one Session.

```yaml
apiVersion: agents.softcane.io/v1alpha1
kind: Agent
metadata:
  name: investigator
  namespace: codex-session-operator-system
spec:
  model: gpt-6-luna
  instructions: Investigate the supplied evidence and report the cause and uncertainty.
  tools: []
  deletionPolicy: Delete
```

This Agent can be referenced by the Profile and direct Session examples below. The [application Agent template](../examples/my-go-project/automation/k8s/agent.yaml.tmpl) shows an Agent with repository skills and execution tools. See the [Agent API source](../api/v1alpha1/agent_types.go) and generated schema for the full reasoning, output, multi-agent and tool variants.

## Environment

| Mode | Execution | Per-Session Kubernetes resources |
| --- | --- | --- |
| `None` | OpenAI reasoning and permitted service tools, without an execution filesystem. | Session CR only. The first durable message accompanies remote creation. |
| `OpenAIHosted` | OpenAI-managed runtime created from the Environment template. | Session CR; OpenAI reports runtime readiness. |
| `SelfHosted` | OpenAI runs the agent while `codex exec-server` supplies commands and files from your cluster. | Session CR, SandboxClaim, Sandbox, executor Pod and template-defined storage. |

A text-only Environment has no mode-specific branch:

```yaml
apiVersion: agents.softcane.io/v1alpha1
kind: Environment
metadata:
  name: text-only
  namespace: codex-session-operator-system
spec:
  mode: None
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
  mode: OpenAIHosted
  deletionPolicy: Delete
  openaiHosted:
    name: hosted-small
    containerSize: small
    network:
      access: disabled
```

A self-hosted Environment selects an approved, same-namespace SandboxWarmPool. Its SandboxTemplate defines the executor image, preparation, tools, resources, security and storage. The SandboxTemplate owns those settings.

```yaml
apiVersion: agents.softcane.io/v1alpha1
kind: Environment
metadata:
  name: go-workspace
  namespace: codex-session-operator-system
spec:
  mode: SelfHosted
  deletionPolicy: Delete
  selfHosted:
    sandboxPoolRef:
      name: my-go-project-v1
    capabilityDirectories:
      - /capabilities/my-go-project
    workspace:
      directory: /workspace
      deletionPolicy: Retain
```

| Field | Meaning |
| --- | --- |
| `selfHosted.sandboxPoolRef.name` | Required, approved SandboxWarmPool in the same namespace. |
| `selfHosted.workspace.directory` | Workspace location exposed to OpenAI; defaults to `/workspace`. |
| `selfHosted.workspace.deletionPolicy` | Required `Delete` or `Retain` policy for Session cleanup of the Sandbox environment. |
| `selfHosted.capabilityDirectories` | Directories where the template exposes skills or other capabilities to the runtime. |

The template must actually provide the declared files and directories. See the [Environment sample](../config/samples/agents_v1alpha1_environment_selfhosted.yaml), [application template](../examples/my-go-project/automation/k8s/sandbox.yaml.tmpl) and [deployment guide](deployment.md#5-configure-a-workspace-and-profile).

Workspace paths must be clean absolute directories outside reserved runtime paths. Capability paths must be clean absolute paths. References use same-namespace DNS subdomain names. Pool approval, live template identities, credentials and resource availability remain reconciliation checks.

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
    mode: Limited
    limitCents: 500
  vaultRefs: []
```

Profile readiness means its selected policy resources are Ready. Starting a task through the conversation API freezes the Profile identity, policy and selected dependency identities before the gateway creates a Session.

## Spending policy

Profiles and Sessions require an explicit `spendControl`:

| Mode | Required configuration | Behavior |
| --- | --- | --- |
| `Limited` | Positive `limitCents`, in US cents. | Requests a per-Session cap. Requires provider-project support and verified readback before readiness and later delivery. |
| `Unlimited` | Omit `limitCents`. | Explicitly selects unlimited Session spending. |

In `None` mode, the first input is part of remote Session creation, so the first turn starts before cap readback. The controller reports `SpendControlUnavailable` when the provider does not support the requested cap; it does not switch to unlimited spending. See [spending verification](../CONTRIBUTING.md#choose-a-spending-mode).

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
    mode: Limited
    limitCents: 500
  vaultRefs: []
```

The gateway can create the same resource through `POST /v1/namespaces/{namespace}/sessions`:

```json
{
  "name": "manual-investigation",
  "spec": {
    "agentRef": {"name": "investigator"},
    "environmentRef": {"name": "text-only"},
    "spendControl": {"mode": "Limited", "limitCents": 500},
    "vaultRefs": []
  }
}
```

The route returns `201` with the Session URL. A `None` Environment waits for durable initial input. Send it to `POST /v1/namespaces/{namespace}/sessions/{name}/messages` with an idempotency key, then read `/history` for the saved result.

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

## Desired state, updates and observation

### Omission, empty values and resets

Optional provider settings distinguish omitted/unmanaged fields, explicit empty replacements and explicit provider-default resets. `clearName`, `clearScope`, `clearTools`, `resetReasoning` and `resetServiceTier` express persistent desired clear/default state. They are reconciled idempotently; false or omission does not request a reset. Tools, skills, packages, setup commands and process arguments are ordered atomic lists. Set/map lists such as allowed hosts/domains, capability directories, files by path and environment variables by name have order-independent comparisons and digests.

### Session update rules

Before its remote creation claim, a Session may change its references and creation settings. After `status.creationAttempted` is persisted, admission freezes Agent, Environment and Vault selection, instructions, tools, text, multi-agent configuration and reasoning summary. Existing Sessions may update model, reasoning effort, service tier, metadata and explicit spending policy. Editing reusable dependencies affects future Sessions; existing Sessions keep their frozen identity and recipe.

Opaque JSON Schemas in function parameters and text formats are also creation-only. Kubernetes CEL equality can admit some same-sized replacements of unknown schema keys. The controller compares its frozen creation digest after admission, reports `ConfigurationCurrent=False` with `RequiresNewSession`, and preserves the original remote configuration and identity. Revert that edit or create a new Session. This controller check also protects against drift independently of admission.

### Conditions

Conditions are independent observations: `RemoteSessionCreated` describes the owned remote object's existence, `ConfigurationCurrent` describes desired configuration, `WorkspacePrepared` describes the verified current executor process/workspace and `EnvironmentConnected` describes the provider connection; `Ready` summarizes current eligibility. Supported settings updates do not erase remote existence or verified workspace/connection facts. Definite remote absence, ownership changes and lost execution identity revoke the relevant execution proofs. During cleanup, confirmed remote absence remains false while compute or workspace cleanup is pending.

Consumers must check the relevant condition's `observedGeneration` against `metadata.generation`. Condition types and reasons are open-ended: ignore unknown types and handle unfamiliar reasons without assuming failure or inventing a phase ordering. `Session.status` has no phase; the gateway's `summary.phase` is a display summary derived from conditions. A Ready Session or display phase does not establish turn completion; read durable root-turn history.

## Vault credentials and MCP tools

Vault credential values come from the fixed same-namespace `codex-vault` Secret. The CR records the protected source identity and provider credential ID, without copying the secret value into status.

Create the Secret separately, then apply a [Vault](../config/samples/agents_v1alpha1_vault.yaml) and [bearer credential](../config/samples/agents_v1alpha1_vaultcredential.yaml). Configure an Agent's service-origin HTTP MCP tool with the same server URL and attach the Vault through `Profile.spec.vaultRefs`.

When one attached credential matches the MCP URL, the controller selects it. If several match, set `Agent.spec.tools[].mcp.credentialID` to the provider ID in `VaultCredential.status.openaiCredentialID`. The application OpenAI key also needs `api.vaults.read` and `api.vaults.write`; see [OpenAI authentication](https://developers.openai.com/api/docs/guides/agents-api/environments/self-hosted#authentication).

`static_bearer` and `mcp_oauth` credentials apply to service-origin HTTP MCP tools. `environment_variable` credentials use `environmentVariable.variableName` to name a hosted environment variable, not a Kubernetes Secret. They supply placeholders to supported hosted environments; they do not inject values into self-hosted executor Pods. The executor connection key in `codex-executor` has a separate purpose.

## Deletion and ownership

Reusable Agent, Environment, Vault and VaultCredential resources use explicit `Delete` or `Retain` policies. Profile deletion removes only the reusable binding. A Session always cleans up its owned remote conversation and, when self-hosted, its executor before its finalizer completes.

Self-hosted workspace policy is set at `Environment.spec.selfHosted.workspace.deletionPolicy`:

| Policy | Cleanup behavior | What remains |
| --- | --- | --- |
| `Delete` | Remove the SandboxClaim and wait for its dependent execution resources to disappear. | Gateway conversation history. |
| `Retain` | Suspend the Sandbox, wait for its Pod to disappear, detach it from the claim, then delete the claim. | Gateway history, the suspended Sandbox and its persistent files for administrative inspection. |

A retained Sandbox does not attach to a later Session. Delete it explicitly when its files are no longer needed. The StorageClass volume reclaim policy and backup policy are separate controls.

The controller keeps creation claims and finalizers when a remote create or delete outcome is uncertain. Recover the exact object through its Kubernetes UID and verified owner scope; do not clear claims or finalizers to force progress.

## Related documentation

- [Architecture](architecture.md) explains lifecycle and state ownership.
- [Conversations](conversations.md) defines Profile-backed task, retry and completion semantics.
- [Deployment](deployment.md) covers images, Secrets, PostgreSQL and executor policy.
- [CRD schemas](../config/crd/bases/) remain the field-level contract.
