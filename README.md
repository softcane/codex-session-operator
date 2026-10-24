# Codex Session Operator

Run OpenAI coding agents against repositories in Kubernetes. Define reusable agent and workspace configuration, then submit a task through the gateway. The operator creates a Session for the conversation, and the gateway saves messages, history and approvals in PostgreSQL.

OpenAI runs the agent loop. For self-hosted execution, [Kubernetes Agent Sandbox](https://agent-sandbox.sigs.k8s.io/) runs `codex exec-server` in your cluster, where it edits files and executes commands. You control the executor image, repository revision, tools, credentials and persistent storage.

The API is alpha. Start with the simulated demo to try the lifecycle without an API key, or follow the repository walkthrough to run a real coding task.

## Try the local demo

From this checkout, with Docker running and Go, Make, Kind, kubectl, Python 3.10+, Node.js and OpenSSL installed, run:

```sh
make demo
```

Use the Go toolchain specified in [go.mod](go.mod). Make downloads pinned build tools into `bin/` as needed.

The demo builds an isolated Kind cluster with the controller, gateway, PostgreSQL, Agent Sandbox and a simulated OpenAI provider. It exercises conversations and resource cleanup, then keeps the successful deployment for inspection. It needs no API key and makes no model calls.

After a successful run, follow the printed `env.sh` and port-forward instructions to reach the gateway. The command also prints the report path and the command to delete its cluster. See [Use the retained demo](docs/development.md#use-the-retained-demo) for a CLI example. To run the same suite and remove the cluster afterward, use `make test-e2e`.

## Run a real repository task

Follow [Run my-go-project](docs/my-go-project.md) to ask an agent to reproduce and fix a bug in a Go service, verify the change and export its commit. The walkthrough covers image builds, credentials, PostgreSQL, installation and the durable CLI.

You need a Kubernetes cluster, an OpenAI project with Agents API access, a separate restricted executor key and PostgreSQL. Model calls incur charges; the walkthrough's Profile explicitly selects unlimited spending. For an existing platform, use the [deployment guide](docs/deployment.md).

## Configure once, start conversations as needed

All six custom resources use `agents.softcane.io/v1alpha1`. References stay within a Kubernetes namespace.

| Resource | Purpose |
| --- | --- |
| `Agent` | Model, reusable instructions, reasoning settings and tools. |
| `Environment` | Execution mode and workspace policy. Self-hosted environments select an approved Sandbox pool. |
| `Profile` | Agent, Environment, optional Vaults and an explicit spending policy that callers select when starting work. |
| `Session` | One conversation attempt, normally created by the gateway from a Profile. |
| `Vault` | Provider credential container for connected tools. |
| `VaultCredential` | Provider credential configuration backed by Kubernetes Secret references. |

Applying an Agent, Environment and Profile creates no task Pod. A caller starts work by selecting a Ready Profile and sending a message with a stable idempotency key. Each new self-hosted conversation gets its own Session and workspace; follow-up messages use the existing conversation.

Administrators define executor images, repository preparation, resources, Secret mounts and storage in a `SandboxTemplate`. A `SandboxWarmPool` selects that template, and the controller's `SANDBOX_POOLS` setting approves the pool. Managed templates and pools have immutable specifications; use versioned names when changing them. See the [resource reference](docs/resources.md) and [complete application templates](examples/my-go-project/automation/k8s/).

## How a task runs

![Self-hosted lifecycle: configure a Profile, accept input into PostgreSQL, create a Session and SandboxClaim, run commands in an Agent Sandbox executor, read history and clean up](docs/assets/session-flow.gif)

[View the static overview](docs/assets/session-flow.png).

1. **Accept input.** The gateway commits the message and retry receipt to PostgreSQL, then returns HTTP `202` with a conversation URL.
2. **Create the Session.** A gateway worker creates the Kubernetes Session. The controller resolves Ready dependencies, freezes their identities and creation settings, and records a claim before creating the remote OpenAI Session.
3. **Prepare execution.** For a `SelfHosted` Environment, the controller creates a Session-owned SandboxClaim. Agent Sandbox creates the Sandbox, executor Pod and workspace storage from the approved template.
4. **Run the task.** After verifying ownership, repository preparation, workspace identity and a current OpenAI connection, the gateway delivers input. OpenAI runs the agent and requests commands from the executor.
5. **Read the outcome.** The gateway saves turns and output. Read the new root turn and its results to determine whether the task succeeded.
6. **Continue or close.** Follow-up messages keep the Session and workspace. Close stops new input and requests cleanup; a fresh attempt after cleanup creates a new Session and workspace.

HTTP `202` confirms durable acceptance. Completion requires a later history read. When retrying an uncertain request, keep its idempotency key and body unchanged. The [conversation guide](docs/conversations.md) explains retries, completion checks and approvals.

A replacement executor reconnects to the same Session and persistent workspace. On close, `workspace.deletionPolicy: Delete` removes the workspace; `Retain` leaves a suspended Sandbox and its files for administrative inspection. The claim is removed in both cases. PostgreSQL keeps conversation history after Session cleanup.

## Install and operate

Deploy the controller and gateway together as separate Deployments and images. Provision PostgreSQL separately, and install the Agent Sandbox core and extensions separately for self-hosted execution. The Go dependency and deployment examples pin **Agent Sandbox v1.0.7**.

The gateway serves HTTP by default. Your deployment layer provides authentication, access control and any required TLS termination. Callers who can reach a namespace's gateway routes share access to its conversations. See [deployment](docs/deployment.md) for installation, approved pools, storage and recovery.

## Documentation and development

| To… | Read |
| --- | --- |
| Run a real coding task from setup to exported commit | [Repository walkthrough](docs/my-go-project.md) |
| Install and operate the services | [Deployment](docs/deployment.md) |
| Start, continue, retry and close conversations | [Conversations and CLI](docs/conversations.md) |
| Look up CRD configuration and update rules | [Resource reference](docs/resources.md) |
| Understand ownership, readiness and recovery | [Architecture](docs/architecture.md) |
| Build and choose verification checks | [Development](docs/development.md) |
| Contribute a change or publish a release | [Contributing](CONTRIBUTING.md) |
| Report a vulnerability | [Security](SECURITY.md) |

For local development:

```sh
make lint
make test
make test-helpers
```

To regenerate the lifecycle images, install `Pillow>=10.4,<13` and run `python3 docs/assets/generate-session-flow.py`.

## License

[Apache License 2.0](LICENSE).
