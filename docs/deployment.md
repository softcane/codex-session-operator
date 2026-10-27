# Install in your cluster

You provide a Kubernetes cluster, PostgreSQL, two OpenAI keys and images your nodes can pull. The installation runs the controller and gateway as separate Deployments. Agent Sandbox runs each self-hosted workspace.

To run a first repository task after installation, follow [getting started](getting-started.md).

## 1. Select the cluster

Replace the context and create the installation namespace if it does not exist:

```sh
export KUBE_CONTEXT=your-kubernetes-context
export NAMESPACE=codex-session-operator-system
kubectl --context "$KUBE_CONTEXT" create namespace "$NAMESPACE"
```

The manifests use this namespace. Keep it unless you also update the Service, RBAC and internal gateway references in your deployment configuration.

## 2. Create the Secrets

Create the application and environment keys using the [OpenAI key instructions](getting-started.md#1-create-the-openai-keys). Set `OPENAI_API_KEY`, `OPENAI_EXECUTOR_API_KEY` and `DATABASE_URL` in your shell, or provision these Secrets through your Secret manager:

```sh
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" create secret generic openai-application \
  --from-literal=OPENAI_API_KEY="$OPENAI_API_KEY"
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" create secret generic codex-executor \
  --from-literal=CODEX_API_KEY="$OPENAI_EXECUTOR_API_KEY"
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" create secret generic gateway-database \
  --from-literal=url="$DATABASE_URL"
```

The controller and gateway use `openai-application`. Executor Pods use only the restricted key in `codex-executor`. Add `api.vaults.read` and `api.vaults.write` to the application key if you use Vault resources.

### PostgreSQL

Provision a database and supply its connection URL using a database-owner account. The gateway creates its tables automatically. Use a TLS connection with URL-encoded credentials:

```text
postgres://USER:PASSWORD@HOST:5432/DATABASE?sslmode=verify-full
```

If the database uses a private CA, add its certificate as `ca.crt` in `gateway-database` and add `sslrootcert=/gateway-database/ca.crt` to the URL. A separate CA file is unnecessary when the certificate is already trusted by the gateway image. Plaintext database connections are supported only in disposable local mode.

## 3. Install Agent Sandbox

Self-hosted execution requires the separately installed Agent Sandbox core and extensions. This checkout pins v1.0.7:

```sh
kubectl --context "$KUBE_CONTEXT" apply --server-side -f \
  https://github.com/kubernetes-sigs/agent-sandbox/releases/download/v1.0.7/sandbox-with-extensions.yaml
kubectl --context "$KUBE_CONTEXT" wait --for=condition=Established \
  crd/sandboxes.agents.x-k8s.io crd/sandboxclaims.extensions.agents.x-k8s.io \
  crd/sandboxtemplates.extensions.agents.x-k8s.io crd/sandboxwarmpools.extensions.agents.x-k8s.io \
  --timeout=90s
kubectl --context "$KUBE_CONTEXT" -n agent-sandbox-system rollout status \
  deployment/agent-sandbox-controller --timeout=180s
```

## 4. Install the controller and gateway

Choose a published tag from [Releases](https://github.com/softcane/codex-session-operator/releases). Pin that version for the operator, CLI and example configuration:

```sh
export VERSION=vX.Y.Z
kubectl --context "$KUBE_CONTEXT" apply -f \
  "https://github.com/softcane/codex-session-operator/releases/download/${VERSION}/install.yaml"
```

For the latest stable release, the manifest URL is `https://github.com/softcane/codex-session-operator/releases/latest/download/install.yaml`. A published release is required for these URLs.

Set a permanent installation/project identity and approve the example's pool:

```sh
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" set env deployment/codex-session-operator-controller-manager \
  OPENAI_AGENT_OWNER_SCOPE=your-cluster/your-openai-project \
  SANDBOX_POOLS=sandbox-v1
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" rollout status deployment/codex-session-operator-controller-manager --timeout=180s
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" rollout status deployment/codex-session-operator-gateway --timeout=180s
```

Keep `OPENAI_AGENT_OWNER_SCOPE` unchanged across restarts and upgrades. `SANDBOX_POOLS` lists exact approved pool names; an empty list disables self-hosted execution. Configure both before applying Agent or Session CRs.

<details>
<summary>Build the installation manifest from source</summary>

From the checkout, render your published image references and apply the result:

```sh
make build-installer \
  CONTROLLER_IMG=YOUR_REGISTRY/controller@sha256:CONTROLLER_DIGEST \
  GATEWAY_IMG=YOUR_REGISTRY/gateway@sha256:GATEWAY_DIGEST \
  SANDBOX_POOLS=sandbox-v1
kubectl --context "$KUBE_CONTEXT" apply -f dist/install.yaml
```

Rendering requires the Go toolchain in [go.mod](../go.mod), Make and Python 3.10+. The [Makefile](../Makefile) provides image build and push targets. Configure the ownership scope and pool approval shown above before creating agents.

</details>

## 5. Configure a workspace and Profile

Download the [repository example](../examples/repository-task.yaml) from the same release:

```sh
curl -fsSL -o repository-task.yaml \
  "https://github.com/softcane/codex-session-operator/releases/download/${VERSION}/repository-task.yaml"
```

Edit `repository-task.yaml`: replace the executor image digest, node architecture and StorageClass before applying it:

```sh
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" apply -f repository-task.yaml
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" wait --for=condition=Ready \
  agent/sandbox-agent environment/sandbox-environment profile/sandbox --timeout=180s
```

The file contains the SandboxTemplate, `sandbox-v1` pool, Agent, Environment and Profile shown in the [README](../README.md#example-configuration). It pins `google/uuid` at a full commit and explicitly selects `Unlimited` spending. Choose your spending policy before starting work; see [spending policy](resources.md#spending-policy).

The executor image must contain `codex exec-server`, `/workspace-bootstrap` and your project's tools. The [application executor Dockerfile](../examples/my-go-project/sandbox/Dockerfile.executor) adds Go tools to the shared runtime. Repository preparation, resources, Secret mounts and storage belong in the template; the operator supplies Session connection settings.

Managed template and pool specifications are immutable. To change an installed recipe, create a new versioned pair and approve the new pool. Applying the configuration starts no task Pod; a conversation creates its own Session and workspace.

## 6. Install the CLI and connect

Choose the platform for the machine running the client:

| Platform | Value |
| --- | --- |
| Linux x86-64 | `linux_amd64` |
| Linux ARM64 | `linux_arm64` |
| macOS Intel | `darwin_amd64` |
| macOS Apple Silicon | `darwin_arm64` |

For example, on macOS with Apple Silicon:

```sh
export PLATFORM=darwin_arm64
curl -fsSLO "https://github.com/softcane/codex-session-operator/releases/download/${VERSION}/gateway-client_${PLATFORM}.tar.gz"
tar -xzf "gateway-client_${PLATFORM}.tar.gz" gateway-client
sudo install -m 0755 gateway-client /usr/local/bin/gateway-client
gateway-client --help
```

The release's `SHA256SUMS` contains checksums for the CLI archives, installation manifest, example and image metadata. Each CLI archive includes the binary and license.

Keep a gateway port-forward running in another terminal:

```sh
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" port-forward \
  --address 127.0.0.1 service/codex-session-operator-gateway 18082:8082
```

Use `--origin http://127.0.0.1:18082` and your namespace with the client. Follow [send a task](getting-started.md#4-send-a-task), or use the [CLI and HTTP reference](conversations.md).

The gateway and its internal controller endpoint use HTTP by default. A loopback port-forward needs no gateway token, certificate or CA file. The gateway has no caller authentication; your ingress or proxy supplies access control when exposing it.

## Repository preparation and durable client

The Sandbox template pins the Git commit and runs `/workspace-bootstrap`. For private repositories, mount repository credentials in the preparation init container. The [application template](../examples/my-go-project/automation/k8s/sandbox.yaml.tmpl) shows the Secret mounts and a separate skills bundle. Mount publishing credentials in the executor only when the task needs permission to push branches or create PRs.

Repository files and local commits live on the workspace PVC. Conversation input, history and receipts live in PostgreSQL. Retrieve changes before closing a workspace with the `Delete` policy. With `Retain`, close stops compute and removes the claim while keeping a suspended Sandbox and its files for inspection. Delete that Sandbox explicitly when finished.

Keep the client's operation journal to recover uncertain requests. Keep `GATEWAY_SUBJECT` stable across replicas and restarts if you override its default, `gateway`. After rotating `openai-application`, restart the controller and gateway Deployments to reload it. A replacement executor loads the current `codex-executor` value.

For additional Session namespaces, grant the gateway the same scoped Pod/Sandbox observation Role there. Configure storage, network egress and runtime isolation through your cluster and Sandbox template. PostgreSQL and workspace storage need separate backups.

## Optional transport and monitoring

An ingress can terminate TLS. Direct gateway TLS uses `GATEWAY_CERT` and `GATEWAY_KEY`, HTTPS probes and matching controller trust settings. These are optional deployment choices.

The `config/monitoring` overlay enables HTTPS metrics, ServiceMonitors and the dashboard. It requires Prometheus Operator and a `gateway-tls` Secret. That Secret belongs to monitoring and is not required by the default installation.

## Storage and recovery

Manage PostgreSQL backups separately. The gateway requires `fsync`, `full_page_writes` and `synchronous_commit` enabled; these preserve accepted input across restarts.

All replicas must use matching quotas: defaults are 32 active execution slots and 100,000 retained records, configured by `--gateway-max-active` and `--gateway-max-records`. Closed history consumes retention capacity. Replay identities persist after archive.

Use `/gateway-admin` from the gateway image in an offline maintenance Job with the gateway database Secret mounted. Treat backups as private conversation data. The commands below are separate maintenance operations; choose the one you need.

If deployed quotas differ from the defaults, pass the matching `-max-active` and `-max-records` to this administrative command. Archive removes payloads from the selected closed record while retaining receipts, replay fingerprints, approval decisions and operation or cleanup identities. Conversation and Session records are archived separately, and backups retain their own copies.

```sh
/gateway-admin -action report -database-file /gateway-database/url
/gateway-admin -action backup -database-file /gateway-database/url -path /private/backup.json
/gateway-admin -action restore -database-file /gateway-database/url -path /private/backup.json
/gateway-admin -action resume -database-file /gateway-database/url -path /private/audit.json
/gateway-admin -action archive -database-file /gateway-database/url -path closed-record-uid
```

Restore requires an empty database and disables fresh delivery. Before resuming, reconcile Kubernetes claims, provider outcomes and recovery logs, including remote actions missing from the backup. The audit needs one decision per reported record: `uid`, its current `digest`, a nonempty `reference`, and either `verified:true` or `blocked:true`. Unresolved records retain their claims and remain held. Reads and exact receipt replay stay available.

Backup serialization buffers the dataset in memory. Primary-volume loss, standby promotion and production restore objectives require separate qualification. A successful process restart does not establish zero-loss database failover.

## Current limits

This is an alpha API. The no-key end-to-end suite proves the deployment and conversation lifecycle with fixed simulated responses. It does not prove real inference, executor tools, provider budget enforcement or production availability.

Kubernetes namespaces do not isolate remote Agents or Vaults within a shared OpenAI project. Bind provider credentials and project ownership separately before multi-tenant use. Hosted runtimes, MCP reconnection and optional tool variants also need deployment-specific verification.

Conversations support text, saved turns, approvals, close and restart. Retrieve repository files from the executor before deleting its workspace; the gateway does not provide downloadable workspace artifacts. Clients must retain their operation journal to recover uncertain requests.
