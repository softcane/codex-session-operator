# Deployment

Use the [my-go-project walkthrough](my-go-project.md) for self-hosted execution with Profile CRDs and the Go CLI. You need an OpenAI project with Agents API access, Kubernetes, PostgreSQL and TLS. Self-hosted execution also needs a separate restricted executor credential. Real model calls incur charges.

The supported installation always deploys the controller and gateway together. They remain separate Deployments and images so they can carry only their own binaries and permissions. Applying a prebuilt `install.yaml` requires `kubectl`; building manifests and repository test harnesses from source also requires Python 3.10 or newer. PostgreSQL is a prerequisite rather than part of the installation: use an external service such as Amazon RDS, or provision the local example for development.

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

### Gateway HTTPS certificate

Use a certificate from your PKI for an established deployment. It must cover `codex-session-operator-gateway.codex-session-operator-system.svc` for internal RPC and the address your clients use. For a local port-forward, the following private self-signed certificate covers that service name and `127.0.0.1`; clients and the controller explicitly trust it.

```bash
umask 077
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 30 \
  -keyout .private/setup/gateway.key -out .private/setup/gateway.crt \
  -subj '/CN=codex-session-operator-gateway' \
  -addext 'subjectAltName=DNS:codex-session-operator-gateway.codex-session-operator-system.svc,IP:127.0.0.1'
cp .private/setup/gateway.crt .private/setup/ca.crt

kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" create secret generic gateway-tls \
  --from-file=tls.crt=.private/setup/gateway.crt \
  --from-file=tls.key=.private/setup/gateway.key \
  --from-file=ca.crt=.private/setup/ca.crt \
  --dry-run=client -o yaml | kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" apply -f -
```

Supply the real certificate chain and CA bundle instead when using your PKI. Keep all three Secret keys: a Kubernetes `tls` Secret alone does not provide the controller's required `ca.crt`. Renew the certificate before expiry and roll out the services to load updated configuration.

### Caller and controller tokens

Generate different tokens for the public caller and internal RPC. This also writes a private curl configuration for local requests; the OpenAI key stays with the controller and gateway.

```bash
python3 - <<'PY'
import json, os, secrets
from pathlib import Path
d = Path('.private/setup')
caller, internal = secrets.token_hex(32), secrets.token_hex(32)
files = {
    'principals.json': json.dumps([{
        'token': caller, 'subject': 'quickstart-user',
        'namespace': os.environ['NAMESPACE'], 'approver': False, 'admin': False
    }]),
    'internal-token': internal,
    'curl.conf': 'header = "Authorization: Bearer ' + caller + '"\n'
}
for name, value in files.items():
    p = d / name
    p.touch(mode=0o600, exist_ok=True)
    p.chmod(0o600)
    p.write_text(value)
PY

kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" create secret generic gateway-auth \
  --from-file=principals.json=.private/setup/principals.json \
  --dry-run=client -o yaml | kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" apply -f -
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" create secret generic gateway-internal \
  --from-file=token=.private/setup/internal-token \
  --dry-run=client -o yaml | kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" apply -f -
```

Rerunning token generation rotates credentials; keep the files for client retries. A caller-token change requires a gateway rollout. An internal-token or gateway trust-CA change requires both controller and gateway rollouts, since both cache that RPC configuration. Application-key rotation also requires both Pods to reload their environment.

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

`make build-installer` renders the combined CRDs, controller Deployment, and gateway Deployment into `dist/install.yaml`. It does not provision PostgreSQL or create Secrets. Maintain image references, a stable ownership scope, credential names, executor allowlists, repository allowlists, and storage policy in your deployment overlay.

```sh
make build-installer \
  CONTROLLER_IMG='YOUR_REGISTRY/controller@sha256:CONTROLLER_DIGEST' \
  GATEWAY_IMG='YOUR_REGISTRY/gateway@sha256:GATEWAY_DIGEST' \
  EXECUTOR_IMG='YOUR_REGISTRY/executor@sha256:EXECUTOR_DIGEST'

kubectl --context YOUR_CONTEXT apply -f dist/install.yaml
```

`make deploy KUBE_CONTEXT=YOUR_CONTEXT` renders and applies the same manifest. Gateway replicas serialize fresh schema initialization through PostgreSQL, so the combined manifest needs no separate database Job.

Select the namespace, images, and Secret configuration before installation. The examples use `codex-session-operator-system`; update the namespace and service DNS names together if you choose another namespace. The gateway NetworkPolicy restricts internal RPC to controller Pods; public API traffic still requires HTTPS and caller authentication.

## Required Secrets

Create these through your Secret manager or private manifests. Keep values out of Git and terminal output.

| Secret | Keys | Consumer |
| --- | --- | --- |
| `openai-application` | `OPENAI_API_KEY` | Controller and gateway, using the same OpenAI project. |
| `gateway-auth` | `principals.json` | Gateway callers and roles. |
| `gateway-internal` | `token` | Separate controller-to-gateway authentication. |
| `gateway-tls` | `tls.crt`, `tls.key`, `ca.crt` | Gateway HTTPS and controller verification. |
| `gateway-database` | `url`, optionally `ca.crt` | PostgreSQL connection used for schema initialization and gateway data. |
| `codex-executor` | `CODEX_API_KEY` | Restricted environment-connection credential in each self-hosted Session namespace. |

The gateway certificate must cover `codex-session-operator-gateway.codex-session-operator-system.svc`. The controller verifies that service through the mounted CA. The public listener uses port `8082`; internal lifecycle RPC uses `8083`.

Caller configuration uses this shape; replace the token before deployment:

```json
[
  {
    "token": "<random-caller-token>",
    "subject": "team-member",
    "namespace": "codex-session-operator-system",
    "approver": false,
    "admin": false
  }
]
```

A caller's namespace determines access. Subjects within that namespace share conversation access. `approver` enables approval decisions; `admin` enables administrative routes. Port-forwarding does not grant a role. OIDC login is not implemented.

## Controller and profiles

Set a stable `OPENAI_AGENT_OWNER_SCOPE` for this installation and provider project. Retain it across restarts and upgrades: the controller uses it with Kubernetes UIDs to verify remote ownership.

For self-hosted execution, follow the dedicated setup below. `EXECUTOR_IMAGES` approves application executor digests; `CAPABILITY_IMAGES` approves capability bundles. `WORKSPACE_STORAGE_CLASSES` selects allowed StorageClasses.

Declare a same-namespace `Profile` selecting Ready Agent and Environment resources, optional Vaults and an explicit spend policy. The gateway reads current Profile CRDs without a restart. Profile readiness is policy readiness, not proof that a sandbox task ran. Accepted attempts freeze Profile and dependency identities; the controller verifies them while resolving creation snapshots. See the [README example](../README.md#my-go-project-example) and [resource reference](resources.md#profile).

`limited` requires a positive US-cent cap and provider-project support. The controller verifies readback before readiness and later delivery. In `none` mode, the first turn starts during remote creation, before cap readback. `unlimited` is an explicit spending choice. The local demo uses it with a simulated provider.

Vault credentials use protected same-namespace references to the fixed `codex-vault` Secret. Hosted protected environment inputs use `codex-environment`. Vault environment-variable credentials do not inject values into self-hosted Pods. See [resource samples](../config/samples/) and the generated [CRD schemas](../config/crd/bases/) for field validation.

## Self-hosted execution

OpenAI runs the managed harness; a Session-specific Pod supplies command execution through `codex exec-server`. An Environment CR describes the recipe. The Session controller creates compute when a Session selects it.

1. Build and publish the shared `executor` target for the selected architecture. Extend it with the application toolchain, publish that image, and record its immutable digest.
2. Add the application executor digest to the controller's comma-separated `EXECUTOR_IMAGES` allowlist. Add capability image digests through `CAPABILITY_IMAGES` when using separate skills or plugin bundles.
3. For a persistent workspace, include its StorageClass in comma-separated `WORKSPACE_STORAGE_CLASSES`. An empty list denies persistent workspace provisioning.
4. Create a namespace-local Opaque Secret called `codex-executor`, containing only `CODEX_API_KEY`. Use a separate restricted environment-connection credential; the controller rejects reuse of the application key.
5. Apply a `selfHosted` Environment with explicit `workspaceStorage` (`Ephemeral` or `Persistent`), then select it in a new Session or Profile.

Use [environment-persistent.yaml](../config/samples/agents_v1alpha1_environment_persistent.yaml) for the field shape. Replace its image and StorageClass placeholders. The Session becomes Ready after the Pod is Running/Ready and OpenAI reports a fresh environment connection for that executor.

An ephemeral workspace resets with Pod replacement. A persistent PVC survives replacement on the same Session. Its `claimDeletionPolicy` decides whether Session cleanup deletes or retains the claim. A retained claim deliberately has no Session owner reference, and a new Session does not automatically adopt it. The StorageClass backing-volume reclaim policy and backups remain separate concerns.

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

A self-hosted Environment can declare `selfHosted.repository.url`, a complete 40-character `commit`, and optional `credentialSecretRef` selecting only the fixed same-namespace `codex-repository` Secret. The controller's `REPOSITORY_URLS` / `--repository-urls` approves exact credential-free HTTPS URLs. Empty approval denies repository preparation. Redirects, unsafe URL forms and arbitrary Secret names are rejected. The application executor must inherit `/workspace-bootstrap` and Git from the shared executor. Project tools such as Go belong in the application layer.

By default, the bootstrap init container mounts repository credentials only for HTTPS preparation; the executor receives only its separate `codex-executor` key. Set `selfHosted.repository.mountCredentialsInExecutor: true` to explicitly mount the selected repository credential files read-only at `/repository-credentials` in the executor as well. This requires `credentialSecretRef`. Runtime Git uses `/workspace-bootstrap` as a `GIT_ASKPASS` helper that checks the HTTPS endpoint. Git can request credentials without a repository path, so hosting-provider permissions must enforce repository scope. Tools such as a GitHub CLI wrapper can read the mounted token file for explicitly authorized branch pushes and pull request creation. The image must supply those tools; the operator does not create pull requests itself.

Runtime credential mounting exposes the token to task commands. Use hosting-provider permissions restricted to the intended repository and operations; this field does not enforce token scope or branch permissions. Keep the token and optional username in Secret files, never URL userinfo, arguments, CR status or Events. Protected source UID/resource version participates in dependency readiness; no credential value enters the snapshot. With runtime mounting enabled, same-UID Secret rotation replaces the executor Pod while preserving a persistent workspace. Missing, invalid or recreated protected sources stop the executor and hold readiness, including during provider outages; a different Secret UID is not silently adopted by an existing Session.

Preparation creates a pinned project subtree and Session/repository marker in the workspace root. Replacement Pods validate the existing marker and preserve tracked edits, deletions, untracked files and local commits. They never reset, clean, pull or silently recreate an ambiguous workspace. A missing/malformed/foreign marker or failed init holds delivery and requires explicit recovery. `WaitForFirstConsumer` storage schedules the Pod before claim binding.

Set `WORKSPACE_STORAGE_CLASSES` to approved classes and `EXECUTOR_IMAGES` to approved application executor manifest digests. Persistent claims belong to the Session UID; `Delete` removes them during cleanup, while `Retain` preserves the claim without automatically attaching it to another Session. A fresh attempt creates an independent claim. Volume and PostgreSQL backups remain installation responsibilities.

Use `/gateway-client` in the gateway image or `bin/gateway-client` locally. Its token file and state directory must be private. The journal records operation content, subject, namespace and exact replay identity before sending, and persists a receipt before success. See [conversations](conversations.md#cli-and-execution-readiness) for accepted/pending/ambiguous outcomes.

The shipped gateway grants executor `pods/get` through a namespaced Role and RoleBinding only; it has no Pod list/watch or cluster-wide Pod-read grant. If callers manage sandboxes in additional namespaces, provision the same scoped Role and bind the gateway ServiceAccount there. Profile discovery does not grant executor observation in unrelated namespaces.
