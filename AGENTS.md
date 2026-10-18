# Repository guide for coding agents

Read [README.md](README.md) for the user workflow, [docs/architecture.md](docs/architecture.md) for component boundaries, and [docs/conversations.md](docs/conversations.md) for the conversation contract. Check the implementation before changing a contract; the code and generated CRDs define this checkout.

## Repository map

| Path | Responsibility |
| --- | --- |
| `api/v1alpha1/` | The six namespaced CRD APIs: Agent, Environment, Profile, Vault, VaultCredential, Session. |
| `internal/controller/` | Kubernetes reconciliation, remote identity, dependency snapshots, finalizers, executor and workspace lifecycle. |
| `internal/openaiapi/` | OpenAI SDK adapter, provider ownership checks, configuration conversion and error redaction. |
| `internal/gateway/` | Authenticated HTTP, Profile admission, durable conversations and history, approvals, PostgreSQL storage and worker fencing. |
| `internal/sandbox/` | Validation and construction of approved executor Pods and workspaces. |
| `cmd/main.go`, `cmd/gateway/`, `cmd/gateway-admin/`, `cmd/gateway-client/`, `cmd/workspace-bootstrap/` | Controller, gateway, offline administration, durable caller and repository preparation binaries. |
| `config/` | Kubebuilder manifests, overlays and example custom resources; `config/demo/` uses a simulated provider. |
| `hack/` | Copyright boilerplate and release installer rendering. |
| `test/e2e/`, `test/` | One isolated Kind end-to-end suite, its simulated provider and client, plus database and helper tests. |
| `docs/` | Public architecture, deployment, API and contributor documentation. |

## Rules that protect the system's contract

- Keep prompts, turns, event bodies, approvals, replay keys and transcripts in the gateway store. CR status carries remote IDs, conditions, frozen safe configuration and source versions, not conversation content or credential values.
- A new Session needs same-namespace `agentRef` and `environmentRef` plus explicit `spendControl`. Resolve Ready dependencies before freezing their identities and creation settings. A `none` Environment needs durable initial gateway input before remote Session creation.
- Treat remote create, update and delete outcomes as potentially ambiguous. Persist one-time claims before remote creates; recover by Kubernetes UID and verified owner scope. Do not clear a claim, replace a missing remote identity or remove a finalizer to bypass an uncertain outcome.
- Keep self-hosted executor Pods and workspace claims tied to the Session UID. Validate approved image digests, storage classes and Secret references. Preserve `Delete`/`Retain` policy behavior and stop gateway input before cleanup.
- Use a separate restricted executor credential. Never log, commit or put Secret contents, API keys or generated demo credentials in status, Events, reports or examples.
- A gateway HTTP `202` means input was durably accepted. Turn completion needs a later history read. Use stable idempotency keys for retries, and reject changed content under the same identity.
- Ship the controller and gateway together as separate Deployments and images. Provision PostgreSQL separately; the gateway initializes its tables at startup under a database lock so replicas can start safely together. Keep the initial schema in `internal/gateway/schema.sql`.

## Editing and verification

1. Update API source in `api/v1alpha1/`, not generated CRDs, RBAC or `zz_generated.deepcopy.go`. Keep Kubebuilder scaffold markers and `PROJECT` intact. Run `make manifests generate` after API markers change.
2. Match tests to the changed boundary: Go unit/envtest for reconciliation, provider wire tests for remote behavior, PostgreSQL tests for durability, and isolated Kind tests for deployed Pod, PVC and network behavior. Do not use a shared Kubernetes context for destructive tests.
3. For Go changes run `make lint` and `make test` when feasible. `make test-e2e` runs the disposable Kind lifecycle suite with a simulated provider; `make demo` retains that deployment for inspection. Use real PostgreSQL tests for changes to transaction or recovery behavior. Real OpenAI verification uses separate restricted credentials and may incur charges.
4. Keep README examples, architecture diagrams and public API documentation aligned with code. Check sample YAML against the CRD schema and document whether a command uses the simulated provider or real OpenAI.

`go.mod` specifies the language and toolchain versions. Read `Makefile` before running a target that creates a cluster, installs tools, regenerates files or calls the provider. When changing OpenAI fields, check the pinned SDK and current provider contract before assuming a field's omission, null, mutability or ownership semantics.
