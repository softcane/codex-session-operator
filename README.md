# Codex Session Operator

Run durable OpenAI agent conversations through Kubernetes. Define an Agent and Environment, choose an approved profile, then send a first message. The operator manages the Session lifecycle; the gateway stores messages, receipts, history and approvals in PostgreSQL.

![Choose a profile, save input in the gateway, create a Kubernetes Session and run it through OpenAI](docs/assets/session-flow.gif)

## Try it without an API key

You need Docker, Go matching `go.mod`, Make, Python 3, Node.js, Kind, `kubectl` and OpenSSL.

```sh
make demo
```

This builds an isolated Kind cluster with the operator, two gateway replicas, PostgreSQL and a simulated provider. It checks the lifecycle, then leaves the cluster running. The provider returns fixed replies; the demo makes no real OpenAI inference calls.

Follow the printed `source /…/env.sh` instruction, then start the printed gateway port-forward:

```sh
kubectl -n "$DEMO_NAMESPACE" port-forward --address 127.0.0.1 \
  service/codex-session-operator-gateway "$DEMO_PORT:8082"
```

Open the printed UI URL. The demo uses a self-signed localhost certificate; the runner prints its certificate path. Connect with `$DEMO_NAMESPACE` and the bearer token from the private `$DEMO_DIR/curl.conf`, then choose `demo-investigation`. Send a message, inspect its history, add evidence or close it. The runner prints the exact command to remove its cluster.

You can also start a conversation from the command line:

```sh
python3 -B test/e2e/client.py conversation demo-investigation \
  --key first-task-001 --text 'Explain the supplied evidence.' \
  --output "$DEMO_DIR/receipt.json"
```

## Resource model

| Resource | Purpose |
| --- | --- |
| `Agent` | Reusable model, instructions and tools. |
| `Environment` | Text only, OpenAI hosted or self-hosted execution. |
| `Vault` / `VaultCredential` | Provider credential containers and protected Secret references. |
| `Session` | One conversation bound to ready resources and an explicit spending policy. |

A trusted profile maps the Agent, Environment, optional Vaults and spend policy. Callers supply the task. The gateway creates the Session in the background and returns a stable conversation URL.

The API request looks like this:

```http
POST /v1/namespaces/team/conversations
Authorization: Bearer <namespace-token>
Idempotency-Key: first-task-001
Content-Type: application/json

{"route_id":"approved-profile","text":"Investigate the supplied evidence."}
```

HTTP `202` means the request committed. Read the returned conversation URL to observe turn completion. Retry with the same key and body after a lost response. Close stops new input and cleans up the Session; restart after cleanup creates a new attempt while keeping earlier history readable.

The controller freezes dependency identities before remote creation and uses claims and finalizers to recover uncertain outcomes. Conversation content stays in PostgreSQL. Self-hosted Sessions can use an approved executor image and persistent workspace PVC.

## Documentation

- [Architecture](docs/architecture.md): components, resources and recovery boundaries.
- [Conversations](docs/conversations.md): profiles, API, retries, close and restart.
- [Deployment](docs/deployment.md): images, Secrets, PostgreSQL and current limits.
- [Development](docs/development.md): build, checks and release workflow.

## Development

```sh
make build
make lint
make test
make test-e2e
```

`make test-e2e` runs the isolated simulated-provider suite and removes its cluster. One [Dockerfile](Dockerfile) builds the operator/gateway, executor and test-provider targets.

This project has an alpha API. Real deployments need Agents API access, TLS, protected credentials and a tested database recovery plan. See the [deployment guide](docs/deployment.md) before using real OpenAI resources.

## License

[Apache License 2.0](LICENSE).
