# Deployment

Use the [my-go-project walkthrough](my-go-project.md) for self-hosted execution with Profile CRDs and the Go CLI. You need an OpenAI project with Agents API access, Kubernetes and PostgreSQL. Self-hosted execution also needs a separate restricted executor credential. Real model calls incur charges.

The supported installation always deploys the controller and gateway together. They remain separate Deployments and images so they can carry only their own binaries and permissions. Applying a prebuilt `install.yaml` requires `kubectl`; building manifests and repository test harnesses from source also requires Python 3.10 or newer. Self-hosted execution also requires the separately installed Agent Sandbox core and extensions controllers and their `v1beta1` CRDs. PostgreSQL is a gateway dependency provisioned and managed externally by the platform team, for example through Amazon RDS. It is outside the operator installation and reconciliation lifecycle. Only the demo and local development helpers deploy PostgreSQL in Kubernetes for convenience.

## Configure the gateway Secrets

Create these Secrets before applying the installation manifest. The examples below use `KUBE_CONTEXT`, `NAMESPACE` and a private `.private/setup` directory. The [my-go-project walkthrough](my-go-project.md) shows the complete setup, including `openai-application`.

### PostgreSQL connection and schema

Provision PostgreSQL with `fsync`, `full_page_writes` and `synchronous_commit` enabled. Make the server reachable from gateway Pods, including DNS and the database port through your cluster's network rules.

The gateway database role must own the target schema and have `CONNECT`, schema `USAGE` and `CREATE`, and `SELECT`, `INSERT`, `UPDATE`, and `DELETE` on its tables. On a fresh database, gateway replicas take a PostgreSQL advisory lock and initialize the tables from the embedded `schema.sql`. Other replicas wait for the lock and then verify the initialized schema. A restart keeps existing tables and data.

Obtain the database CA certificate from its administrator and copy it to `.private/setup/database-ca.crt`. Enter the connection URL without exposing credentials in a command argument:

```bash
python3 - <<'PY'
from getpass import getpass
from pathlib import Path
p = Path('.private/setup/database-url')
p.touch(mode=0o600, exist_ok=True)
p.chmod(0o600)
p.write_text(getpass('PostgreSQL URL: ').strip())
PY
```

The URL must select `sslmode=verify-full` and trust the mounted CA. Its shape is `postgres://USER:PASSWORD@DATABASE_HOST:5432/DATABASE?sslmode=verify-full&sslrootcert=/gateway-database/ca.crt`; URL-encode special characters in the username and password. The `sslrootcert` path belongs inside the container rather than your workstation.

```bash
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" create secret generic gateway-database \
  --from-file=url=.private/setup/database-url \
  --from-file=ca.crt=.private/setup/database-ca.crt \
  --dry-run=client -o yaml | kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" apply -f -
```

### Gateway HTTP transport

The public gateway on port `8082` and controller RPC on port `8083` serve HTTP by default. The gateway does not implement caller authentication or roles; the platform team manages access control through its ingress, proxy or network policy. Local port-forwards bind to `127.0.0.1`; no gateway certificate or CA is required.

An ingress or reverse proxy can terminate TLS for external callers. To enable TLS directly in the gateway, mount a certificate and key and set `GATEWAY_CERT` and `GATEWAY_KEY` together, change the readiness probe to HTTPS, and configure the controller's `GATEWAY_INTERNAL_URL` with `https://` and `GATEWAY_INTERNAL_CA_FILE` with its trusted CA. TLS-enabled CLI callers can supply `--ca-file`. Database TLS is independent of gateway HTTP.

### Stable replay identity

No caller or controller token Secrets are required. Requests use the namespace in their URL. `caller_subject` remains in API responses and journals as a stable service replay identifier, defaulting to `gateway`; it is not an authenticated user identity. Keep `GATEWAY_SUBJECT` identical across replicas and restarts for a given database.

When replacing an older token-based deployment, resolve outstanding operations before changing replay identity. Setting `GATEWAY_SUBJECT` to the previous caller subject preserves that subject's existing keys and journals. Other subjects' journals are rejected on identity mismatch rather than silently replayed under a new identity. Saved conversations remain readable through their namespace and ID.

Application-key rotation requires controller and gateway Pods to reload their environment.

## Images and manifests

The root multi-stage Dockerfile builds separate production images:

| Target | Contents |
| --- | --- |
| `controller` | `/manager` only. |
| `gateway` | `/gateway`, `/gateway-admin`, and `/gateway-client`. |
| `executor` | Shared self-hosted Codex runtime, Git, certificates, and `/workspace-bootstrap`; no application toolchain. |
| `test-provider`, `test-executor`, `test-repository` | Deterministic fixtures for the isolated end-to-end suite. |

Application repositories extend the shared executor. The example's [Dockerfile.executor](../examples/my-go-project/sandbox/Dockerfile.executor) adds Go, Make, a C compiler, and GitHub CLI. Its [Dockerfile.skills](../examples/my-go-project/sandbox/Dockerfile.skills) builds a separate immutable skills bundle.

```sh
make docker-build-controller CONTROLLER_IMG=codex-controller:local
make docker-build-gateway GATEWAY_IMG=codex-gateway:local
make docker-build-executor EXECUTOR_IMG=codex-executor:local
make docker-build-application-executor \
  EXECUTOR_IMG=codex-executor:local \
  APPLICATION_EXECUTOR_IMG=my-go-project-executor:local
```

Push the images to your registry and record immutable digests before rendering an installation or Environment.

| Overlay | Includes |
| --- | --- |
| `config/controller` | Controller-only Kubebuilder base used as a building block. |
| `config/gateway` | Gateway, internal RPC, RBAC and NetworkPolicy, built on `config/controller`. |
| `config/default` | Public combined installation based on `config/gateway`. |
| `config/monitoring` | Gateway plus ServiceMonitors and a Grafana dashboard; requires Prometheus Operator CRDs. |

`make build-installer` renders the combined CRDs, controller Deployment, and gateway Deployment into `dist/install.yaml`. It does not provision PostgreSQL or create Secrets. Maintain image references, a stable ownership scope, credential names, approved sandbox pools, and Sandbox template policy in your deployment overlay.

```sh
make build-installer \
  CONTROLLER_IMG='YOUR_REGISTRY/controller@sha256:CONTROLLER_DIGEST' \
  GATEWAY_IMG='YOUR_REGISTRY/gateway@sha256:GATEWAY_DIGEST' \
  SANDBOX_POOLS='my-go-project-v1'

kubectl --context YOUR_CONTEXT apply -f dist/install.yaml
```

`make deploy KUBE_CONTEXT=YOUR_CONTEXT` renders and applies the same manifest. Gateway replicas serialize fresh schema initialization through PostgreSQL, so the combined manifest needs no separate database Job.

Select the namespace, images, and Secret configuration before installation. The examples use `codex-session-operator-system`; update the namespace and service DNS names together if you choose another namespace. The gateway NetworkPolicy restricts internal RPC to controller Pods; configure public API access control in your deployment layer. Expose external HTTPS through your ingress when needed.

## Required Secrets

Create these through your Secret manager or private manifests. Keep values out of Git and terminal output.

| Secret | Keys | Consumer |
| --- | --- | --- |
| `openai-application` | `OPENAI_API_KEY` | Controller and gateway, using the same OpenAI project. |
| `gateway-tls` (optional) | `tls.crt`, `tls.key`, `ca.crt` | Authenticated HTTPS metrics when using the monitoring overlay; direct gateway TLS requires an explicit overlay. |
| `gateway-database` | `url`, optionally `ca.crt` | PostgreSQL connection used for schema initialization and gateway data. |
| `codex-executor` | `CODEX_API_KEY` | Restricted environment-connection credential in each self-hosted Session namespace. |

The public listener uses port `8082`; internal lifecycle RPC uses `8083`. The installed NetworkPolicy restricts internal RPC to controller Pods. Direct TLS is optional; when configured, the certificate must cover the service name and the controller must trust its CA.

## Controller and profiles

Set a stable `OPENAI_AGENT_OWNER_SCOPE` for this installation and provider project. Retain it across restarts and upgrades: the controller uses it with Kubernetes UIDs to verify remote ownership.

For self-hosted execution, follow the dedicated setup below. `SANDBOX_POOLS` approves the sandbox setups callers can select. Administrators control template images, repositories, storage classes, Secret references and network policy.

Declare a same-namespace `Profile` selecting Ready Agent and Environment resources, optional Vaults and an explicit spend policy. The gateway reads current Profile CRDs without a restart. Profile readiness is policy readiness, not proof that a sandbox task ran. Accepted attempts freeze Profile and dependency identities; the controller verifies them while resolving creation snapshots. See the [Profile resource reference](resources.md#profile).

`Limited` requires a positive US-cent cap and provider-project support. The controller verifies readback before readiness and later delivery. In `None` mode, the first turn starts during remote creation, before cap readback. `Unlimited` is an explicit spending choice. The local demo uses it with a simulated provider.

Vault credentials use protected same-namespace references to the fixed `codex-vault` Secret. Hosted protected environment inputs use `codex-environment`. Vault environment-variable credentials do not inject values into self-hosted Pods. See [resource samples](../config/samples/) and the generated [CRD schemas](../config/crd/bases/) for field validation.

## Self-hosted execution

OpenAI runs the managed harness; Agent Sandbox supplies a Session-specific workspace running `codex exec-server`. Our controller creates a `SandboxClaim` and verifies its execution identity. It does not construct Pods or provision PVCs.

1. Install compatible Agent Sandbox core and extensions controllers and the `agents.x-k8s.io/v1beta1` and `extensions.agents.x-k8s.io/v1beta1` CRDs. The walkthrough installs the pinned [v1.0.7 combined manifest](https://github.com/kubernetes-sigs/agent-sandbox/releases/download/v1.0.7/sandbox-with-extensions.yaml) with server-side apply.
2. Build and publish an immutable executor image containing the application's tools. Create the separate restricted `codex-executor` Secret in the Session namespace; do not reuse the application's OpenAI key.
3. Apply an administrator-maintained SandboxTemplate and SandboxWarmPool in that namespace. The template defines the executor image, resource requests, architecture, security, preparation, Secret mounts and storage. Set `replicas: 0` on the pool and `envVarsInjectionPolicy: Allowed` on its template for on-demand Session allocation.
4. Approve the pool with the controller's comma-separated `SANDBOX_POOLS` configuration (exact pool names). Only administrators should be able to create approved pools and templates. Use versioned names and label both resources `agents.softcane.io/managed: "true"`. The combined installation includes an admission policy that prevents changes to managed template and pool specifications, including label removal. Create a new versioned setup for configuration changes.
5. Apply a `SelfHosted` Environment selecting `sandboxPoolRef.name`, workspace and capability directories, and `workspace.deletionPolicy: Delete` or `Retain`. Select it in a new Session or Profile.

See [the minimal Sandbox template and pool](../config/samples/sandbox_template.yaml), [the application template](../examples/my-go-project/automation/k8s/sandbox.yaml.tmpl) and [Environment](../config/samples/agents_v1alpha1_environment_selfhosted.yaml). The template must provide an `executor` container that starts `codex exec-server` using the injected `CODEX_ENVIRONMENT_ID` and `CODEX_REMOTE_URL`. Claims also inject `CODEX_SESSION_UID` for Session binding and `SESSION_UID` into a `repository-prepare` init container when configured. Credentials stay in template Secret references. A fixed repository commit and preparation command belong in the template.

Per-claim environment variables require fresh creation even if the pool has spare Pods. This integration starts on demand; it does not promise warm-pool startup speed. A future warm allocation design would need a separate post-start Session assignment protocol.

The Session becomes Ready only after the ownership chain, current Pod and workspace, template-defined preparation and fresh OpenAI connection have been verified. Sandbox readiness alone is insufficient. Agent Sandbox replaces missing Pods while template-defined persistent storage preserves files. `workspace.deletionPolicy: Delete` deletes the claim and waits for its resources to disappear. `Retain` suspends the Sandbox, waits for compute to stop, detaches the Sandbox from its claim, and deletes the claim. Its persistent files and stopped environment remain for administrative inspection; later Sessions get new workspaces. Delete the retained Sandbox explicitly when its files are no longer needed.

Runtime isolation and egress are template/cluster policy. Install gVisor or Kata and select the appropriate RuntimeClass when required; installing Agent Sandbox alone does not enable either runtime. Configure network access for OpenAI, Git, package sources and any approved services. StorageClass reclaim policy and backups remain separate responsibilities.

## Storage and recovery

Provision PostgreSQL outside the gateway overlay. Give the gateway role the ownership and privileges described in [PostgreSQL connection and schema](#postgresql-connection-and-schema). The URL must use `sslmode=verify-full`, a trusted CA and no plaintext fallback; `sslrootcert=/gateway-database/ca.crt` references the mounted CA. Plaintext is reserved for disposable local tests.

The gateway checks `fsync`, `full_page_writes` and `synchronous_commit` and requires them on. All replicas must use matching quotas: defaults are 32 active execution slots and 100,000 retained records, configured by `--gateway-max-active` and `--gateway-max-records`. Closed history consumes retention capacity. Replay identities persist after archive. The gateway prunes commit-outcome markers older than one hour in batches of at most 1,000, with a one-second query bound and a minute between sweeps when no backlog remains. These markers are separate from durable caller replay identities; their retention exceeds the longest administrative transaction and its outcome check.

Use `/gateway-admin` from the gateway image in an offline maintenance Job with the gateway database Secret mounted. Treat backups as private conversation data.

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

Conversations support text, saved turns, approvals, close and restart. A2A, outbound callbacks, retained downloadable artifacts and broker subscriptions are not implemented. Clients must save operation keys and input to recover uncertain requests across process restarts.

## Repository preparation and durable client

Repository preparation is configured in the SandboxTemplate, not in the Environment API. The supplied example pins a complete repository commit and uses `/workspace-bootstrap` to prepare the checkout. It mounts repository credentials only in the init container by default, separately from the executor connection key. Keep tokens and optional usernames in Secret files, never URL userinfo, arguments, CR status or Events.

For explicitly authorized publishing tasks, an administrator can mount the repository credential read-only in the executor and configure the Git authentication helper and GitHub CLI wrapper. Hosting-provider permissions must restrict the repository and allowed operations. The operator does not create pull requests itself. Template ownership and cluster admission are the approval boundary for image digests, repository URLs, storage classes and credential mounts.

Preparation creates a pinned project subtree and Session/repository marker in the workspace root. Replacement Pods validate that marker and preserve tracked edits, deletions, untracked files and local commits. They must not reset or silently recreate an ambiguous workspace. Failed preparation or changed compute/storage identities hold delivery. Persistent volumes and PostgreSQL need independent backup policies.

Use `/gateway-client` in the gateway image or `bin/gateway-client` locally. Its operation state directory must be private. The journal records operation content, subject, namespace and exact replay identity before sending, and persists a receipt before success. See [conversations](conversations.md#cli-and-execution-readiness) for accepted/pending/ambiguous outcomes.

The shipped gateway grants namespaced observation of executor Pods and their Sandbox ownership resources through a Role and RoleBinding. If callers manage sandboxes in additional namespaces, provision the same scoped Role and bind the gateway ServiceAccount there. Profile discovery does not grant executor observation in unrelated namespaces.
