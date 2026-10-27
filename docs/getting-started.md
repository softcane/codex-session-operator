# Run your first repository task

Run a managed Codex Session against `google/uuid` using the released [example manifest](../examples/repository-task.yaml). OpenAI runs the agent; commands execute in your Kubernetes cluster.

You need `kubectl`, `jq` and `gateway-client`. Follow [installation](deployment.md) to install the operator and download the CLI for your machine.

## 1. Create the OpenAI keys

Your OpenAI project needs Agents API access, access to `gpt-6-luna` and billing for inference. Create two keys:

| Key | Create it in | Purpose |
| --- | --- | --- |
| `OPENAI_API_KEY` | Your project's API key settings. Grant `api.agents.read`, `api.agents.write` and `api.responses.write`. | Controller and gateway requests. |
| `OPENAI_EXECUTOR_API_KEY` | [Agents → Environment keys](https://platform.openai.com/agents?tab=environments&environment_view=keys). Create an environment-connection key and set every other permission to **None**. | Connects `codex exec-server` to the managed Session. |

Use the same organization, project and user or service account for both keys. See [OpenAI's authentication instructions](https://developers.openai.com/api/docs/guides/agents-api/environments/self-hosted#authentication). Provide the keys through the [Kubernetes Secrets](deployment.md#2-create-the-secrets) used by the installation.

## 2. Apply the configuration

Select your installed cluster explicitly:

```bash
export KUBE_CONTEXT=your-kubernetes-context
kubectl --context "$KUBE_CONTEXT" get nodes
```

Download the example from the same published version you installed:

```bash
export VERSION=vX.Y.Z
curl -fsSL -o repository-task.yaml \
  "https://github.com/softcane/codex-session-operator/releases/download/${VERSION}/repository-task.yaml"
```

Edit `repository-task.yaml`: replace the executor image digest, node architecture and StorageClass. The executor image needs Go, `codex exec-server` and `/workspace-bootstrap`. The controller's `SANDBOX_POOLS` setting must include `sandbox-v1`.

The example Profile explicitly selects `Unlimited`, which incurs API charges without a per-Session dollar cap. If your project supports Session caps, you can instead select `Limited` with a positive `limitCents`. Choose the policy before starting work; the operator never switches modes automatically.

Apply the Sandbox template, pool, Agent, Environment and Profile:

```bash
kubectl --context "$KUBE_CONTEXT" -n codex-session-operator-system \
  apply -f repository-task.yaml
kubectl --context "$KUBE_CONTEXT" -n codex-session-operator-system \
  wait --for=condition=Ready agent/sandbox-agent \
  environment/sandbox-environment profile/sandbox --timeout=180s
```

No task Pod starts yet. The [README](../README.md#example-configuration) explains these CRs. The template checks out `google/uuid` at commit `0f11ee6918f41a04c201eceeadf612a377bc7fbc` when a Session requests its workspace.

## 3. Connect to the gateway

In a **second terminal**, start the port-forward using the same Kubernetes context:

```bash
kubectl --context your-kubernetes-context -n codex-session-operator-system \
  port-forward --address 127.0.0.1 service/codex-session-operator-gateway 18082:8082
```

Keep it running. The gateway is available at `http://127.0.0.1:18082` through this tunnel.

## 4. Send a task

In the **first terminal**, submit the task and save its receipt:

```bash
printf '%s\n' \
  'In /workspace, run pwd, git rev-parse HEAD, hostname and go test ./... .' \
  'Report actual results. Leave source and tests unchanged. Do not delegate.' | \
gateway-client --origin http://127.0.0.1:18082 \
  --namespace codex-session-operator-system \
  --state-dir "$HOME/.local/state/codex-session-operator" \
  start --profile sandbox --text-file - --operation first-task > receipt.json
CONVERSATION_ID="$(jq -er .receipt.conversation_id receipt.json)"
```

The client creates its operation journal at the specified `--state-dir`. Keep it to recover lost responses. The receipt confirms saved input; execution happens afterwards.

To retry that exact operation after a lost response:

```bash
gateway-client --origin http://127.0.0.1:18082 \
  --namespace codex-session-operator-system \
  --state-dir "$HOME/.local/state/codex-session-operator" resume first-task
```

Use a new operation name for each new task or message.

## 5. Read the result

```bash
gateway-client --origin http://127.0.0.1:18082 \
  --namespace codex-session-operator-system read "$CONVERSATION_ID" > result.json
jq '{lifecycle, turns: .history.turns}' result.json
```

Repeat the read while the executor starts. Once a root turn appears, watch it:

```bash
ROOT_ID="$(jq -er '[.history.turns[]? | select(.subagent_id == null)] | last | .id // error("No root turn yet; read again")' result.json)"
gateway-client --origin http://127.0.0.1:18082 \
  --namespace codex-session-operator-system \
  watch "$CONVERSATION_ID" --root-id "$ROOT_ID" --timeout 15m
gateway-client --origin http://127.0.0.1:18082 \
  --namespace codex-session-operator-system read "$CONVERSATION_ID" > result.json
jq '.history.items[]? | select(.type == "command_execution") | {command, exit_code, output}' result.json
```

Check for `/workspace`, the pinned commit, the executor hostname and a passing `github.com/google/uuid` test result with exit code `0`. The managed verification ran these commands successfully and independently checked the repository and tests. Inspect the saved command output to assess success.

Use `continue` for follow-up work in the same workspace, with a new operation name. Observe a new root for each message. See [conversations](conversations.md) for the command and HTTP reference.

## 6. Close and clean up

Save any results or repository changes you need first; this workspace uses `Delete` on close.

```bash
gateway-client --origin http://127.0.0.1:18082 \
  --namespace codex-session-operator-system \
  --state-dir "$HOME/.local/state/codex-session-operator" \
  close "$CONVERSATION_ID" --operation close-first-task
gateway-client --origin http://127.0.0.1:18082 \
  --namespace codex-session-operator-system read "$CONVERSATION_ID" > result.json
jq '.summary | {state, cleaned}' result.json
```

Repeat the read until `state` is `closed` and `cleaned` is `true`. The Session and workspace are removed; saved history remains in PostgreSQL. The Agent, Environment, Profile and pool remain reusable.

To remove the example configuration after all its conversations have closed:

```bash
kubectl --context "$KUBE_CONTEXT" -n codex-session-operator-system \
  delete -f repository-task.yaml --wait=true --timeout=180s
```

Stop the tunnel with Ctrl-C. Keep the client journal and any saved results you need. Leave the controllers running if remote cleanup is unresolved; do not remove finalizers or creation claims to force it.

## If setup or execution fails

| Symptom | Check |
| --- | --- |
| `401` during Agent creation | Check the application key in `openai-application`. The controller logs show provider authentication failures. |
| `401` in executor logs | Check the environment key in `codex-executor` and its project/user ownership. A valid application key does not validate this key. |
| `SpendControlUnavailable` | The project rejected the requested Session cap. Choose a supported spending policy before a new attempt. |
| Task stays queued | Use `kubectl --context "$KUBE_CONTEXT" -n codex-session-operator-system describe session NAME` to inspect preparation and connection conditions. |
| Connection refused | Keep the gateway port-forward running. |

After updating `openai-application`, restart the controller and gateway Deployments to reload it. A replacement executor loads the updated `codex-executor` value. A rejected or uncertain create can retain its claim for audit; keep that claim and its finalizer until the outcome is resolved. See [recovery boundaries](architecture.md#recovery-and-cleanup).

## Use your repository

Before creating a new versioned template and pool, set their Git URL, full commit, executor image and StorageClass. Select that pool in the Environment and approve it in `SANDBOX_POOLS`. Use the [application templates](../examples/my-go-project/automation/k8s/) for private Git credentials and custom skills.
