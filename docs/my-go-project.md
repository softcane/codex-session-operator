# Run `my-go-project` with Codex Session Operator

Use a coding agent to reproduce and fix an idempotency bug in a small Go HTTP service. By the end, you will have a verified commit exported from the executor into your own checkout, plus saved conversation history and test evidence.

This walkthrough uses **real OpenAI execution** and incurs charges. The supplied Profile selects `spendControl.mode: Unlimited`; choose your spending policy before starting a task. For a no-key introduction with simulated responses, use the [local demo](development.md#use-the-retained-demo).

You will:

1. Publish the example repository and build its executor and skills images.
2. Install the controller, gateway, PostgreSQL and Agent Sandbox with the required credentials.
3. Declare an Agent, Environment and Profile, then submit the bug report through the durable CLI.
4. Inspect the new root turn, test results and commit before exporting the work.
5. Optionally replace the executor to check workspace recovery, then close the conversation.

OpenAI runs the agent loop; `codex exec-server` runs commands in your cluster. The controller requests a workspace through a SandboxClaim, and Agent Sandbox creates its Sandbox, Pod and storage. The Profile ties reusable agent and workspace configuration to a spending policy. Applying that configuration creates no task Pod; starting a conversation creates the Session and workspace.

The gateway stores messages, receipts and history in PostgreSQL. Repository edits and local commits live on a separate workspace PVC. See [architecture](architecture.md) for ownership boundaries and the [resource reference](resources.md) for all six CRDs.

## The `my-go-project` example

The [example project](../examples/my-go-project/) is a small Go HTTP job service. Its initial revision contains an idempotency bug: submitting the same request ID twice creates two jobs. The existing tests cover valid and invalid submissions; the agent will add the missing retry regressions, repair the bug, verify and commit the result.

```text
my-go-project/
├── cmd/service/main.go
├── internal/job/                    # Store, HTTP handler and tests
├── go.mod
├── Makefile                        # make bootstrap; make verify
├── AGENTS.md                       # Repository rules and commands
├── docs/architecture.md
├── .agents/skills/
│   ├── golang-how-to/SKILL.md       # This project's Go conventions
│   ├── investigate/SKILL.md
│   ├── fix-bug/SKILL.md
│   └── review-change/SKILL.md
├── sandbox/
│   ├── Dockerfile.executor         # Runtime tools
│   ├── Dockerfile.skills           # Approved skill bundle
│   └── gh-wrapper                  # Optional GitHub publishing helper
└── automation/k8s/
    ├── agent.yaml.tmpl
    ├── environment.yaml.tmpl
    ├── sandbox.yaml.tmpl           # Administrator template and zero-spare pool
    └── profile.yaml.tmpl
```

`AGENTS.md` explains the repository. Each `SKILL.md` describes a procedure the agent can follow. The Agent CR supplies its role and tells it to use those skills. These files are versioned source; they do not create Kubernetes resources by themselves.

The shared Codex executor image supplies `codex exec-server`, Git, repository preparation, and basic runtime tools. The `my-go-project` application executor extends it with Go, Make, a C compiler, and GitHub CLI. The skills image copies `.agents/skills` into `/bundle/skills`. At Session startup, an init container installs that bundle at `/capabilities/my-go-project`; the Environment registers the directory with the agent runtime. Repository checkout alone does not register skills.

```mermaid
flowchart LR
    A[Agent: my-go-maintainer] --> P[Profile: my-go-project]
    E[Environment: my-go-development] --> P
    P --> C[CLI: start a task]
    C --> G[Gateway]
    G <--> D[(PostgreSQL)]
    G --> S[New Session]
    S --> SC[SandboxClaim]
    T[Approved SandboxTemplate + pool] --> B[Agent Sandbox]
    SC --> B
    B --> X[Sandbox + executor Pod + PVC]
    S --> O
    O[OpenAI agent loop] <-->|commands and results| X
```

## Run my-go-project

### Prerequisites

You need:

- A Linux Kubernetes cluster you can administer with `amd64` or `arm64` nodes, a working StorageClass, and outbound access to OpenAI and your Git provider.
- Docker with a builder for your node architecture, and a registry your Kubernetes nodes can pull from. Configure private registry pull credentials before deployment if needed.
- Go matching this repository's `go.mod`, a C compiler for the host race check, Python 3.10 or newer, Git, Make, `kubectl`, `jq`, OpenSSL, `curl`, `envsubst` and GitHub CLI (`gh`). `envsubst` comes from gettext.
- An authenticated GitHub CLI account and Docker registry credentials.
- An external PostgreSQL service with TLS, or enough cluster capacity and storage for the local-development PostgreSQL instance in step 4.
- An application OpenAI key with Agents API and inference access, plus a different restricted environment-connection key from the same provider project. Keep the application key outside executors. See [OpenAI authentication](https://developers.openai.com/api/docs/guides/agents-api/environments/self-hosted#authentication).

Run the steps in order in **Bash**, from this operator checkout, and keep the exported variables for later steps. Replace the placeholder context, StorageClass and registry values before running commands. Steps 1–6 prepare infrastructure; step 7 starts the paid agent task.

### 1. Select the cluster and prepare the project

Choose a new project directory and fill in your context, StorageClass and registry. This walkthrough uses the shipped namespace `codex-session-operator-system`.

```bash
set -euo pipefail
set +x
export OPERATOR_DIR="$(pwd -P)"
export KUBE_CONTEXT=your-kubernetes-context
export NAMESPACE=codex-session-operator-system
export STORAGE_CLASS=your-storage-class
export REGISTRY=your-registry.example.com/your-team
export VERSION=my-go-example-v1
export MODEL=gpt-6-luna
export OWNER_SCOPE=my-cluster/my-go-project
export PROJECT_DIR="$HOME/code/my-go-project"
export PRIVATE_DIR="$OPERATOR_DIR/.private/my-go-project"

k() { kubectl --context "$KUBE_CONTEXT" "$@"; }
k get nodes
k get storageclass "$STORAGE_CLASS"
ARCH="$(k get nodes -o json | jq -er '[.items[].status.nodeInfo | select(.operatingSystem == "linux" and (.architecture == "amd64" or .architecture == "arm64")) | .architecture][0] // error("No supported Linux node")')"
export ARCH

umask 077
mkdir -p "$PRIVATE_DIR/deploy" "$PRIVATE_DIR/author" "$(dirname "$PROJECT_DIR")"
test ! -e "$PROJECT_DIR"
cp -R examples/my-go-project "$PROJECT_DIR"
export PROJECT_DIR="$(cd "$PROJECT_DIR" && pwd -P)"
make -C "$PROJECT_DIR" bootstrap verify
```

Keep `OWNER_SCOPE` stable across controller upgrades. `.private/` is Git-ignored and holds credentials, rendered manifests and the CLI journal. Keep `PRIVATE_DIR` at the shown location so the Kustomize template's relative base path resolves. The canonical paths also satisfy the CLI's protected-file checks.

Publish the example to a new private GitHub repository. Configure your Git author first if your workstation has no identity configured. Use an unused repository name; `gh repo create` will not replace an existing repository.

```bash
export GITHUB_OWNER="$(gh api user --jq .login)"
git -C "$PROJECT_DIR" init -b main
git -C "$PROJECT_DIR" add .
git -C "$PROJECT_DIR" commit -m 'Add my-go-project agent example'
gh repo create "$GITHUB_OWNER/my-go-project" --private \
  --source "$PROJECT_DIR" --remote origin --push

export REPOSITORY_URL="https://github.com/$GITHUB_OWNER/my-go-project.git"
export REPOSITORY_COMMIT="$(git -C "$PROJECT_DIR" rev-parse HEAD)"
REPOSITORY_TOKEN="$(gh auth token)"
printf '%s' "$REPOSITORY_TOKEN" > "$PRIVATE_DIR/repository-token"
unset REPOSITORY_TOKEN
```

If you already published `my-go-project`, set its existing URL and full 40-character commit instead. Use a repository-scoped credential for ongoing operation. The bootstrap init container mounts it during checkout. The token file must contain the token without a trailing newline.

### 2. Build and publish the images

The executor base comes from the root Dockerfile so it includes the compatible runtime and `/workspace-bootstrap`. The project extends it with Go, Make, a C compiler for race tests, and GitHub CLI. Application source comes from Git at Session startup. This walkthrough builds for the selected architecture and constrains the controller, gateway, and executor to matching nodes, including in a cluster with mixed architectures.

```bash
make build kustomize

docker build --platform "linux/$ARCH" --target controller \
  -t "$REGISTRY/codex-controller:$VERSION" "$OPERATOR_DIR"
docker build --platform "linux/$ARCH" --target gateway \
  -t "$REGISTRY/codex-gateway:$VERSION" "$OPERATOR_DIR"
docker build --platform "linux/$ARCH" --target executor \
  -t codex-executor-base:example "$OPERATOR_DIR"
docker build --platform "linux/$ARCH" \
  --build-arg EXECUTOR_BASE=codex-executor-base:example \
  -f "$PROJECT_DIR/sandbox/Dockerfile.executor" \
  -t "$REGISTRY/my-go-executor:$VERSION" "$PROJECT_DIR"
docker build --platform "linux/$ARCH" \
  -f "$PROJECT_DIR/sandbox/Dockerfile.skills" \
  -t "$REGISTRY/my-go-skills:$VERSION" "$PROJECT_DIR"

docker push "$REGISTRY/codex-controller:$VERSION"
docker push "$REGISTRY/codex-gateway:$VERSION"
docker push "$REGISTRY/my-go-executor:$VERSION"
docker push "$REGISTRY/my-go-skills:$VERSION"

pushed_image() {
  docker image inspect "$1:$VERSION" --format '{{json .RepoDigests}}' |
    jq -er --arg repo "$1" \
      'first(.[] | select((split("@")[0]|ltrimstr("docker.io/")) == ($repo|ltrimstr("docker.io/"))))'
}
CONTROLLER_IMAGE="$(pushed_image "$REGISTRY/codex-controller")"
GATEWAY_IMAGE="$(pushed_image "$REGISTRY/codex-gateway")"
export CONTROLLER_REPOSITORY="${CONTROLLER_IMAGE%@*}"
export CONTROLLER_DIGEST="${CONTROLLER_IMAGE#*@}"
export GATEWAY_REPOSITORY="${GATEWAY_IMAGE%@*}"
export GATEWAY_DIGEST="${GATEWAY_IMAGE#*@}"
EXECUTOR_IMAGE="$(pushed_image "$REGISTRY/my-go-executor")"
SKILLS_IMAGE="$(pushed_image "$REGISTRY/my-go-skills")"
export EXECUTOR_IMAGE SKILLS_IMAGE
```

Use immutable digests in the declarations. The controller and gateway Deployments use their own images. The SandboxTemplate selects the application executor; the shared executor was only its build base. Controller and gateway Deployments can use registry pull credentials in their Pod specifications. Executor templates reject `imagePullSecrets`; configure registry access on the cluster nodes or use node-accessible images. The remaining steps assume the nodes can pull these images.

### 3. Provision credentials

Create the namespace if it does not already exist. Enter keys without placing them in shell history or command arguments.

```bash
k get namespace "$NAMESPACE" >/dev/null 2>&1 || k create namespace "$NAMESPACE"

read -r -s -p 'Application OpenAI key: ' OPENAI_API_KEY
printf '\n'
read -r -s -p 'Restricted executor key: ' OPENAI_EXECUTOR_API_KEY
printf '\n'
printf '%s' "$OPENAI_API_KEY" > "$PRIVATE_DIR/application-key"
printf '%s' "$OPENAI_EXECUTOR_API_KEY" > "$PRIVATE_DIR/executor-key"
unset OPENAI_API_KEY OPENAI_EXECUTOR_API_KEY

k -n "$NAMESPACE" create secret generic openai-application \
  --from-file="OPENAI_API_KEY=$PRIVATE_DIR/application-key"
k -n "$NAMESPACE" create secret generic codex-executor \
  --from-file="CODEX_API_KEY=$PRIVATE_DIR/executor-key"
k -n "$NAMESPACE" create secret generic codex-repository \
  --from-file="token=$PRIVATE_DIR/repository-token"

```

The gateway serves HTTP. A loopback port-forward needs no gateway token or certificate. External deployments manage authentication and TLS at their ingress or proxy.

### 4. Provision PostgreSQL with authentication and TLS

PostgreSQL is a gateway dependency managed externally by your platform team in production. For demo and local development only, the [PostgreSQL manifest](../examples/platform/postgres.yaml.tmpl) creates one Kubernetes database instance and a persistent volume. It enables TLS, password authentication and the durability settings the gateway requires. Skip this manifest when using an external service such as Amazon RDS; supply a gateway role that owns the target schema and can create and use its tables.

```bash
openssl rand -hex 32 | tr -d '\n' > "$PRIVATE_DIR/database-password"
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout "$PRIVATE_DIR/database.key" -out "$PRIVATE_DIR/database.crt" \
  -subj /CN=my-go-postgres \
  -addext "subjectAltName=DNS:my-go-postgres.$NAMESPACE.svc"

k -n "$NAMESPACE" create secret generic my-go-postgres \
  --from-file="password=$PRIVATE_DIR/database-password"
k -n "$NAMESPACE" create secret generic my-go-postgres-tls \
  --from-file="tls.crt=$PRIVATE_DIR/database.crt" \
  --from-file="tls.key=$PRIVATE_DIR/database.key"

envsubst '${NAMESPACE} ${STORAGE_CLASS}' \
  < examples/platform/postgres.yaml.tmpl > "$PRIVATE_DIR/postgres.yaml"
k apply -f "$PRIVATE_DIR/postgres.yaml"
k -n "$NAMESPACE" rollout status deployment/my-go-postgres --timeout=180s
```

Build the verified TLS URL and create the gateway Secret. On first startup, gateway replicas serialize table initialization with a PostgreSQL advisory lock.

```bash
jq -rn --rawfile password "$PRIVATE_DIR/database-password" \
  --arg host "my-go-postgres.$NAMESPACE.svc" \
  '"postgres://codex:"+($password|rtrimstr("\n")|@uri)+"@"+$host+":5432/codex?sslmode=verify-full&sslrootcert=/gateway-database/ca.crt"' \
  > "$PRIVATE_DIR/database-url"
k -n "$NAMESPACE" create secret generic gateway-database \
  --from-file="url=$PRIVATE_DIR/database-url" \
  --from-file="ca.crt=$PRIVATE_DIR/database.crt"
```

For an external service, give the gateway role `CONNECT`, schema ownership with `USAGE` and `CREATE`, and `SELECT`, `INSERT`, `UPDATE`, and `DELETE` on its tables. Plan database and volume backups separately.

### 5. Install the controller and gateway

Install Agent Sandbox with its extensions first, then render the [Kustomize template](../examples/platform/operator.kustomization.yaml.tmpl). It selects the separate controller and gateway images, approves the `my-go-project-v1` sandbox pool, and selects live Profile CRDs.

```bash
k apply --server-side -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/v1.0.7/sandbox-with-extensions.yaml
k -n agent-sandbox-system rollout status deployment/agent-sandbox-controller --timeout=180s
```

```bash
envsubst '${CONTROLLER_REPOSITORY} ${CONTROLLER_DIGEST} ${GATEWAY_REPOSITORY} ${GATEWAY_DIGEST} ${OWNER_SCOPE} ${ARCH}' \
  < examples/platform/operator.kustomization.yaml.tmpl \
  > "$PRIVATE_DIR/deploy/kustomization.yaml"
./bin/kustomize build "$PRIVATE_DIR/deploy" > "$PRIVATE_DIR/install.yaml"
k apply -f "$PRIVATE_DIR/install.yaml"
k wait --for=condition=Established --timeout=90s \
  crd/agents.agents.softcane.io crd/environments.agents.softcane.io \
  crd/profiles.agents.softcane.io crd/sessions.agents.softcane.io \
  crd/vaults.agents.softcane.io crd/vaultcredentials.agents.softcane.io
k -n "$NAMESPACE" rollout status deployment/codex-session-operator-controller-manager --timeout=180s
k -n "$NAMESPACE" rollout status deployment/codex-session-operator-gateway --timeout=180s
```

If gateway startup cannot initialize or read the schema, inspect the Pod logs and database permissions, fix the cause, and apply the same manifest again. Existing tables and data survive gateway restarts.

### 6. Declare the Agent, Environment and Profile

Read the project templates before applying them:

- [Agent](../examples/my-go-project/automation/k8s/agent.yaml.tmpl): `my-go-maintainer` reads repository rules and uses the investigate, fix and review skills.
- [Sandbox template and pool](../examples/my-go-project/automation/k8s/sandbox.yaml.tmpl): administrator configuration for the approved images, pinned repository, 4 GiB workspace, security, and on-demand allocation.
- [Environment](../examples/my-go-project/automation/k8s/environment.yaml.tmpl): `my-go-development` selects the approved pool and uses `workspace.deletionPolicy: Retain`.
- [Profile](../examples/my-go-project/automation/k8s/profile.yaml.tmpl): `my-go-project` ties the two together with an explicit spending policy.

The Profile binding is:

```yaml
spec:
  agentRef:
    name: my-go-maintainer
  environmentRef:
    name: my-go-development
  spendControl:
    mode: Unlimited
  vaultRefs: []
```

Render outside the project Git repository. This pins an existing commit without trying to commit a manifest containing its own commit SHA.

```bash
for name in sandbox agent environment profile; do
  envsubst '${NAMESPACE} ${MODEL} ${EXECUTOR_IMAGE} ${ARCH} ${STORAGE_CLASS} ${REPOSITORY_URL} ${REPOSITORY_COMMIT} ${SKILLS_IMAGE}' \
    < "$PROJECT_DIR/automation/k8s/$name.yaml.tmpl" \
    > "$PRIVATE_DIR/author/$name.yaml"
done

k apply --dry-run=server -f "$PRIVATE_DIR/author"
k apply -f "$PRIVATE_DIR/author"
k -n "$NAMESPACE" wait --for=condition=Ready --timeout=240s \
  agent/my-go-maintainer environment/my-go-development profile/my-go-project
```

At this point, the Agent, Environment and Profile should be Ready, with no task executor Pod. The supplied SandboxTemplate mounts `codex-repository` in preparation. For a public repository, remove that Secret mount and its credential arguments from the template. Ready confirms that the declared dependencies and policy are available. The first task proves checkout and execution. For PR publication, enable runtime credentials before applying these declarations; see [publication](#allow-the-agent-to-publish-a-pr).

### 7. Submit the bug-fix task

Forward the gateway's HTTP service and configure the client. Keep this Bash session open while the background port-forward runs.

```bash
export GATEWAY_PORT=18082
k -n "$NAMESPACE" port-forward --address 127.0.0.1 \
  service/codex-session-operator-gateway "$GATEWAY_PORT:8082" \
  > "$PRIVATE_DIR/port-forward.log" 2>&1 &
FORWARD_PID=$!
curl --fail --silent --show-error --retry 30 --retry-delay 1 --retry-connrefused \
  "http://127.0.0.1:$GATEWAY_PORT/readyz"

client() {
  "$OPERATOR_DIR/bin/gateway-client" --origin "http://127.0.0.1:$GATEWAY_PORT" \
    --namespace "$NAMESPACE" \
    --state-dir "$PRIVATE_DIR/operations" "$@"
}
client profiles show my-go-project

cat > "$PRIVATE_DIR/task.txt" <<'TASK'
Use fix-bug to investigate why two POST /jobs requests with the same request_id
create two jobs. Retries must return the original job and keep one stored job;
different request IDs must create different jobs.
Read AGENTS.md and the registered golang-how-to skill. Add store and HTTP
regressions, demonstrate their failure before repair, fix the code, run make
verify and review the diff. Save logs as .artifacts/regression-before.log,
.artifacts/regression-after.log and .artifacts/make-verify.log.
Create local branch softcane/fix-job-idempotency and commit verified source.
Use repository-local Git author Codex Session Operator Example
<example@example.invalid> if no identity is configured. Return the commit,
test results and limitations. Do not push, open a PR or inspect credentials.
TASK

client start --profile my-go-project --text-file "$PRIVATE_DIR/task.txt" \
  --operation fix-job-idempotency > "$PRIVATE_DIR/receipt.json"
CONVERSATION_ID="$(jq -r .receipt.conversation_id "$PRIVATE_DIR/receipt.json")"
client read "$CONVERSATION_ID" > "$PRIVATE_DIR/conversation.json"
```

The gateway returns acceptance after committing the task. Its worker creates the Session; the controller freezes the selected configuration, creates the remote Session and creates the SandboxClaim. Agent Sandbox provisions its executor/PVC from the template. Init containers install skills and prepare the repository. The gateway waits for preparation and a current execution connection before delivering the task.

### 8. Observe the actual outcome

The receipt from step 7 confirms saved input. A completed root turn confirms finished execution; read its final message and command exit codes to decide whether the task succeeded. Send one task at a time in this example.

This shell function finds a new root and watches that exact ID. It avoids treating an earlier completed turn as the result of a later message.

```bash
watch_new_root() {
  local before="$1" root_id attempt
  for ((attempt=0; attempt<180; attempt++)); do
    client read "$CONVERSATION_ID" > "$PRIVATE_DIR/conversation.json" || return "$?"
    root_id="$(jq -r --argjson before "$before" \
      '[.history.turns[]? | select(.subagent_id==null) | select(.id as $id | $before | index($id) | not)] | last | .id // empty' \
      "$PRIVATE_DIR/conversation.json")"
    if test -n "$root_id"; then
      client watch "$CONVERSATION_ID" --root-id "$root_id" --timeout 15m || return "$?"
      client read "$CONVERSATION_ID" > "$PRIVATE_DIR/conversation.json" || return "$?"
      return 0
    fi
    sleep 2
  done
  printf 'No new root observed; retain the operation and inspect state.\n' >&2
  return 1
}

watch_new_root '[]'
jq -r '.history.items[]? | select(.role=="assistant" and .phase=="final_answer") | .content[]? | .text // empty' \
  "$PRIVATE_DIR/conversation.json"
jq '.history.items[]? | select(.type=="command_execution") | {command,exit_code,output}' \
  "$PRIVATE_DIR/conversation.json"
```

Expect a failing regression before repair, passing store/HTTP regressions afterwards, and a successful `make verify` containing race tests, vet and build. Inspect the actual saved evidence before accepting the result.

After a lost response or caller crash, use `client resume fix-job-idempotency` with the same origin, namespace, caller identity and journal. It replays the saved request. Keep each operation's content unchanged; choose a new operation ID for a new task.

### 9. Bring the verified change back

After checking the test results, export the committed branch and inspect it in your own project checkout. This also gives you the code before you close its executor.

```bash
SESSION_NAME="$(jq -r .summary.session_name "$PRIVATE_DIR/conversation.json")"
SESSION_UID="$(jq -r .summary.session_uid "$PRIVATE_DIR/conversation.json")"
POD_NAME="$(k -n "$NAMESPACE" get session "$SESSION_NAME" -o jsonpath='{.status.podName}')"
k -n "$NAMESPACE" exec "$POD_NAME" -c executor -- \
  git bundle create .artifacts/result.bundle --all
k -n "$NAMESPACE" exec "$POD_NAME" -c executor -- \
  cat .artifacts/result.bundle > "$PRIVATE_DIR/result.bundle"
git -C "$PROJECT_DIR" fetch "$PRIVATE_DIR/result.bundle" \
  refs/heads/softcane/fix-job-idempotency:refs/heads/softcane/agent-result
git -C "$PROJECT_DIR" switch softcane/agent-result
git -C "$PROJECT_DIR" diff main...softcane/agent-result
make -C "$PROJECT_DIR" verify
```

### 10. Continue work and verify workspace recovery

An open conversation keeps its Session and workspace. This optional check replaces its Sandbox-owned executor Pod, then asks the resumed agent to verify its commit and files.

```bash
PREVIOUS_ROOTS="$(jq '[.history.turns[]? | select(.subagent_id==null) | .id]' "$PRIVATE_DIR/conversation.json")"
SESSION_NAME="$(jq -r .summary.session_name "$PRIVATE_DIR/conversation.json")"
SESSION_UID="$(jq -r .summary.session_uid "$PRIVATE_DIR/conversation.json")"
POD_NAME="$(k -n "$NAMESPACE" get session "$SESSION_NAME" -o jsonpath='{.status.podName}')"
k -n "$NAMESPACE" delete pod "$POD_NAME" --wait=true

cat > "$PRIVATE_DIR/recovery.txt" <<'TASK'
Verify the workspace after executor replacement. Confirm the prior commit and
clean worktree, check that .artifacts/regression-before.log survived, and rerun
make verify. Report actual results. Do not edit source or create another commit.
TASK
client continue "$CONVERSATION_ID" --text-file "$PRIVATE_DIR/recovery.txt" \
  --operation verify-after-replacement
watch_new_root "$PREVIOUS_ROOTS"
```

Agent Sandbox creates the replacement using the persistent workspace and the same OpenAI connection settings. PostgreSQL retains the conversation. Losing an executor during a command can leave that command's outcome unknown; the agent must inspect state before repeating side effects.

For a separate investigation, start another conversation from the Profile with an investigation task. That conversation gets an independent workspace at the pinned repository commit.

### 11. Close the conversation

```bash
client close "$CONVERSATION_ID" --operation close-my-go-task
for ((attempt=0; attempt<120; attempt++)); do
  client read "$CONVERSATION_ID" > "$PRIVATE_DIR/conversation.json"
  if jq -e '.summary.state=="closed" and .summary.cleaned==true' \
    "$PRIVATE_DIR/conversation.json" >/dev/null; then
    break
  fi
  sleep 2
done
jq -e '.summary.state=="closed" and .summary.cleaned==true' "$PRIVATE_DIR/conversation.json"
kill "$FORWARD_PID"
```

Close stops fresh input and removes the remote Session and executor. The example's `Retain` policy suspends and keeps the Sandbox and workspace PVC, then removes the claim; saved history remains readable. After `summary.cleaned` becomes `true`, a fresh attempt can create a new Session and workspace using the current Profile. The retained Sandbox does not attach to that new Session.

After exporting any work you need, remove the example declarations with `k -n "$NAMESPACE" delete profile/my-go-project environment/my-go-development agent/my-go-maintainer`. Delete its retained Sandbox only after preserving the files you need; its PVC remains owned by that Sandbox until then. Keep PostgreSQL and the caller journal for retained history and replay; retire their credentials when the installation is no longer needed.

## Allow the agent to publish a PR

Configure publication access **before starting the task**, so the new Session freezes that capability. In step 6's SandboxTemplate, add read-only repository credential mounts and the Git authentication helper configuration to the executor before applying the declarations. The selected Git credential needs branch-push and PR permissions. The executor image already contains GitHub CLI and a wrapper that reads the mounted token internally.

Before step 7, replace the task's final authorization with permission to commit, push `softcane/fix-job-idempotency` and create one draft PR against `main`. Tell the agent to look for an existing PR from that branch before creating one, reuse it on retries and return its URL. Keep merges and deployment outside that task's authorization.

This opt-in mounts selected repository credential files read-only in the executor. Git uses the repository authentication helper; `gh` loads its token through the image wrapper. Hosting-provider permissions control repository and operation scope. The controller provisions the declared execution environment; the agent runs the publishing commands.

## Development and reference

```bash
make lint test
make -C examples/my-go-project bootstrap verify
```

See [development](development.md) for PostgreSQL, isolated Kubernetes and real-provider verification gates; [architecture](architecture.md) for state ownership; [resources](resources.md) for CRD modes and credentials; [conversations](conversations.md) for HTTP and replay contracts; and [deployment](deployment.md) for infrastructure and credentials. The [CRD schemas](../config/crd/bases/) define all authorable fields.

## License

[Apache License 2.0](../LICENSE).
