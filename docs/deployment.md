# Deployment

Start with `make demo` for an isolated deployment with a simulated provider. A real deployment needs an OpenAI project with Agents API access, Kubernetes, PostgreSQL, TLS and separate application and executor credentials. Real model calls incur provider charges.

## Images and manifests

The repository uses one multi-stage Dockerfile:

| Target | Contents |
| --- | --- |
| `operator` (default) | `/manager`, `/gateway` and `/gateway-store`. The manager and gateway run as separate Deployments using the same image. |
| `executor` | Self-hosted Codex runtime. |
| `test-provider` | Simulated provider for the local end-to-end suite. |

```sh
docker build --target operator -t codex-session-operator:local .
docker build --target executor -t codex-executor:local .
```

Push images to your registry and record their immutable digests before configuring self-hosted execution.

| Overlay | Includes |
| --- | --- |
| `config/default` | Controller, CRDs and RBAC. |
| `config/gateway` | Controller plus gateway, internal RPC, RBAC and NetworkPolicy. |
| `config/work` | Gateway plus the trusted profile route ConfigMap. |
| `config/monitoring` | Gateway plus ServiceMonitors and a Grafana dashboard; requires Prometheus Operator CRDs. |

`make build-installer IMG=YOUR_IMAGE` renders the operator-only installer into `dist/install.yaml`. `make build-gateway-installer IMG=YOUR_IMAGE` renders the full `config/work` overlay into `dist/gateway.yaml`. Neither provisions PostgreSQL or credentials. Supply the configuration below through deployment patches in your own overlay.

```sh
make build-gateway-installer IMG='YOUR_REGISTRY/operator@sha256:YOUR_DIGEST'
# Supply deployment configuration and inspect the rendered manifests first.
kubectl --context YOUR_CONTEXT apply -f dist/gateway.yaml
```

Select the namespace, image and Secret configuration before applying. The examples use `codex-session-operator-system`; update the namespace and service DNS names together if you choose another namespace. The gateway NetworkPolicy restricts internal RPC to controller Pods; public API traffic still requires HTTPS and caller authentication.

## Required Secrets

Create these through your Secret manager or private manifests. Keep values out of Git and terminal output.

| Secret | Keys | Consumer |
| --- | --- | --- |
| `openai-application` | `OPENAI_API_KEY` | Manager and gateway, using the same OpenAI project. |
| `gateway-auth` | `principals.json` | Gateway callers and roles. |
| `gateway-internal` | `token` | Separate manager-to-gateway authentication. |
| `gateway-tls` | `tls.crt`, `tls.key`, `ca.crt` | Gateway HTTPS and manager verification. |
| `gateway-database` | `url`, optionally `ca.crt` | PostgreSQL connection. |
| `codex-executor` | `CODEX_API_KEY` | Restricted environment-connection credential in each self-hosted Session namespace. |

The gateway certificate must cover `codex-session-operator-gateway.codex-session-operator-system.svc`. The manager verifies that service through the mounted CA. The public listener uses port `8082`; internal lifecycle RPC uses `8083`.

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

For self-hosted execution, set `EXECUTOR_IMAGE` to an approved `repository@sha256:digest`. `EXECUTOR_IMAGES` and `CAPABILITY_IMAGES` add approved image digests. `WORKSPACE_STORAGE_CLASSES` lists allowed StorageClasses for persistent workspaces; the empty list denies persistent workspace provisioning. The executor receives the restricted key, not the application key.

Create Ready Agent and Environment resources before using a profile. Edit [config/work/routes.yaml](../config/work/routes.yaml) to select same-namespace resources, optional Vaults and an explicit spend policy. Route changes load on gateway restart. The gateway snapshots a route for each accepted attempt; the controller freezes resolved dependencies before remote creation.

```json
{
  "id": "team-investigation",
  "namespace": "codex-session-operator-system",
  "source": "manual",
  "agent": "approved-investigator-v1",
  "environment": "approved-tools-v1",
  "vaults": [],
  "spend_control": {"mode": "limited", "limitCents": 500}
}
```

`limited` requires a positive US-cent cap and provider-project support. The controller verifies readback before readiness and later delivery. In `none` mode, the first turn starts during remote creation, before cap readback. `unlimited` is an explicit spending choice. The local demo uses it with a simulated provider.

Vault credentials use protected same-namespace references to the fixed `codex-vault` Secret. Hosted protected environment inputs use `codex-environment`. Vault environment-variable credentials do not inject values into self-hosted Pods. See [resource samples](../config/samples/) and the generated [CRD schemas](../config/crd/bases/) for field validation.

## Storage and recovery

Provision PostgreSQL outside the gateway overlay. Its role needs gateway table access and initialization DDL. The URL must use `sslmode=verify-full`, a trusted CA and no plaintext fallback; `sslrootcert=/gateway-database/ca.crt` can reference the mounted CA. Plaintext is an explicit disposable-test option.

The gateway checks `fsync`, `full_page_writes` and `synchronous_commit` and requires them on. All replicas must use matching quotas: defaults are 32 active execution slots and 100,000 retained records, configured by `--gateway-max-active` and `--gateway-max-records`. Closed history consumes retention capacity. Replay identities persist after archive.

Use `/gateway-store` from the operator image in an offline maintenance Job with the database Secret mounted. Treat backups as private conversation data.

```sh
/gateway-store -action report -database-file /gateway-database/url
/gateway-store -action backup -database-file /gateway-database/url -path /private/backup.json
/gateway-store -action restore -database-file /gateway-database/url -path /private/backup.json
/gateway-store -action resume -database-file /gateway-database/url -path /private/audit.json
/gateway-store -action archive -database-file /gateway-database/url -path closed-record-uid
```

Restore requires an empty database and disables fresh delivery. Before resuming, reconcile Kubernetes claims, provider outcomes and recovery logs, including remote actions missing from the backup. The audit needs one decision per reported record: `uid`, its current `digest`, a nonempty `reference`, and either `verified:true` or `blocked:true`. Unresolved records retain their claims and remain held. Reads and exact receipt replay stay available.

Backup serialization buffers the dataset in memory. Primary-volume loss, standby promotion and production restore objectives require separate qualification. A successful process restart does not establish zero-loss database failover.

## Current limits

This is an alpha API. The no-key end-to-end suite proves the deployment and conversation lifecycle with fixed simulated responses. It does not prove real inference, executor tools, provider budget enforcement or production availability.

Kubernetes namespaces do not isolate remote Agents or Vaults within a shared OpenAI project. Bind provider credentials and project ownership separately before multi-tenant use. Hosted runtimes, MCP reconnection and optional tool variants also need deployment-specific verification.

Conversations support text, saved turns, approvals, close and restart. A2A, outbound callbacks, retained downloadable artifacts, broker subscriptions and automatic repository checkout are not implemented. The browser keeps uncertain request state in memory; reload recovery needs a client that saves its keys and input. Complete a coordinated gateway rollout before enabling new conversation records, as described in [conversations](conversations.md#storage-and-upgrades).
