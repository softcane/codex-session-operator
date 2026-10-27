# Contributing

For bug reports, include versions, reproduction steps, expected behavior and redacted logs. Report vulnerabilities through [SECURITY.md](SECURITY.md). For a first task, use [getting started](docs/getting-started.md).

## Build and test

Use the Go toolchain in [go.mod](go.mod). Make downloads pinned tools into `bin/`. Run `make help` to list commands.

Run `make lint` and `make test` for Go changes when feasible. Add the checks that exercise the boundary you changed:

| Command | What it checks | Additional requirements |
| --- | --- | --- |
| `make build` | Builds the controller, gateway, administration, client and workspace helper binaries into `bin/`. | Go toolchain. |
| `make lint` | Runs the pinned custom Go linter. | Go toolchain. |
| `make test` | Unit tests and Kubernetes envtest with race detection and shuffled order. Envtest uses pinned upstream Sandbox CRD fixtures. | Go toolchain and a C compiler for race detection. |
| `make test-helpers` | Python deployment helpers and simulated provider contract tests. | Python 3.10+ and Node.js. |
| `make test-storage-postgres` | Transactions, concurrency, fault, crash and restore behavior against disposable PostgreSQL. | Docker and Python 3.10+. |
| `make test-e2e` | Deployed conversations, resource lifecycles and Sandbox Pods/PVCs in an isolated Kind cluster, using a simulated provider. | Docker, Python 3.10+, Node.js, Kind, kubectl and OpenSSL. |
| `make dependencies-check` | Module checksums and dependency-file drift. | Go toolchain. |
| `make vulncheck` | Reachable known Go vulnerabilities. | Go toolchain and network access. |

`make build` and `make test` also regenerate manifests and deepcopy code, format Go source and run vet. Review generated changes before committing. Make downloads pinned tools as needed; see [Makefile](Makefile) for the target dependencies.

Run the PostgreSQL gate for transaction or recovery changes. Run the isolated end-to-end suite for changes to deployed Pod, PVC or network behavior. The [quality workflow](.github/workflows/quality.yml) runs the Go, PostgreSQL, lint, vulnerability and simulated end-to-end gates in CI.

## Simulated demo

`make demo` runs the isolated end-to-end suite and retains its successful Kind deployment. It uses fixed simulated responses, needs no OpenAI key and makes no model calls. Use it to inspect lifecycle behavior when developing the operator.

The runner prints an `env.sh` path, port-forward instructions, a private report path and the exact cluster-deletion command. Source that file in each terminal before using the printed commands. Ordinary `make test-e2e` removes its cluster. Neither command verifies real inference; [getting started](docs/getting-started.md) uses real OpenAI execution.

## Editing

Read [architecture](docs/architecture.md), the [conversation contract](docs/conversations.md) and [AGENTS.md](AGENTS.md) before changing behavior. Edit API source in `api/v1alpha1/`, then run `make manifests generate`. Preserve scaffold markers and `PROJECT`; keep examples aligned with the generated schemas.

Keep conversation content in PostgreSQL and credentials out of status, Events and reports. Preserve creation claims and finalizers when a remote outcome is uncertain. Agent Sandbox owns Pod/PVC creation; preserve its ownership chain and the gateway's delivery checks.

In a pull request, explain the problem, resulting behavior and verification. Report checks you could not complete. Contributions use [Apache 2.0](LICENSE).

To regenerate the README lifecycle images, install `Pillow>=10.4,<13` and run `python3 docs/assets/generate-session-flow.py`.

## Sandbox qualification

The no-key Kind suite exercises all six CRDs, the deterministic execution protocol, real Session-owned SandboxClaims and upstream-managed Pods/PVCs, private HTTPS Git preparation, current connection/preparation holds, replacement and cleanup. Its provider simulation is a separate boundary from the production `codex exec-server`. Reports retain source hashes and safe identities; credentials, prompts, transcripts and Git authentication material stay private.

### Run the full paid gate

The separate paid gate is `make test-sandbox-live`. Supply `OPENAI_API_KEY` and a distinct restricted `OPENAI_EXECUTOR_API_KEY` through the environment or ignored `.env`. The runner allocates at most 500 US cents across all test Sessions, verifies each limited policy, and never silently falls back to unlimited spending. It uses a pinned real `google/uuid` repository, runs its Go tests, introduces a known regression, asks the agent to diagnose/repair it, and independently checks the actual test result and diff. It verifies a hidden filesystem nonce, dirty/untracked/deleted files, local commits, Pod replacement, gateway/controller restart, independent fresh workspaces and Delete/Retain behavior. Read `python3 -B test/e2e/live.py --help` for runtime options before running.

### Choose a spending mode

The default paid gate requires provider support for limited Session spending. A `Session budget configuration is not enabled` rejection, or an API rejection of the unsupported `spend_control` parameter, leaves the Session with `SpendControlUnavailable`; the gate fails before task execution. Use a project and API that support that setting before rerunning the limited gate. Keep the failed run's recovery ledger and creation claim for audit rather than clearing them to retry.

For an explicitly unlimited verification run with Luna, use `make test-sandbox-live LIVE_ARGS='--without-session-budget --model gpt-6-luna'`. This selects `spendControl.mode: Unlimited` and omits the API Session budget setting. The same bounded tasks, deadlines and ownership/cleanup checks apply, but there is no guaranteed dollar cap. Its report marks spending-limit qualification as false even if execution succeeds. The default limited gate never switches modes automatically.

### Run one repository task

For one paid managed Session/root turn with actual repository commands, saved assistant output, independent test verification and owned cleanup, run `make test-sandbox-live LIVE_ARGS='--single-session --without-session-budget --model gpt-6-luna'`. This focused mode does not qualify live regression repair, restart, Retain or fresh-Session scenarios; the full gate remains available separately. Neither mode adds a token cap.

A live claim, remote create/delete or connection outcome that cannot be verified leaves an auditable blocked result and retains the owned cluster/state when necessary. A simulated pass, accepted receipt or completed root alone does not qualify real repository execution. Controlled private HTTPS Git fixtures verify authenticated preparation and credential containment; they do not establish access to an unrelated external private repository.

### Keep an interactive paid conversation

For an interactive retained Luna conversation, add `--keep` to the focused mode:

```sh
make test-sandbox-live LIVE_ARGS='--single-session --keep --without-session-budget --model gpt-6-luna'
```

After a successful root turn, this leaves the remote Agent and Session, open conversation, Sandbox/Pod/PVC, gateway and PostgreSQL running in the owned Kind cluster. It writes the gateway origin, conversation/messages URLs and access paths in the report. Kubeconfig and connection configuration live separately under `.private/live-access/<run>/` with owner-only permissions. The detached tunnel binds only `127.0.0.1` and survives the runner exiting. This retained result is not a cleanup-qualified run.

Use the printed access directory for follow-ups. Each new message needs its own operation name; reuse that name and the same input when retrying an uncertain request:

```sh
<access>/send-message --text-file task.txt --operation followup-1
<access>/watch-conversation --operation followup-1 --timeout 5m
<access>/read-conversation
```

The wrappers use the production `gateway-client` with a private operation journal. The gateway is an HTTP API that delegates access control to the deployment layer, exposed locally through a loopback port-forward. If the tunnel ends (including after a gateway Pod replacement), run `<access>/manage reconnect`. `<access>/manage status` checks current access without sending a task.

### Clean up the paid deployment

When finished, explicitly run `<access>/manage cleanup`. It commits gateway close, waits for Session/compute/workspace finalization and observed remote Session absence, then deletes dependencies through their controllers and verifies remote Agent absence before deleting the owned cluster. Unknown outcomes, changed UIDs or additional conversations/resources stop cleanup and preserve the installation for audit. Closing the terminal or ending this chat does not request cleanup.

## Releases

The [release workflow](.github/workflows/release.yml) runs the shared quality gates before publishing a `vX.Y.Z` tag, with an optional prerelease suffix such as `v0.1.0-rc.1`. It builds three Dockerfile targets for Linux amd64 and arm64:

| Image | Registry name |
| --- | --- |
| Controller | `ghcr.io/<owner>/<repository>:<version-tag>` |
| Gateway | `ghcr.io/<owner>/<repository>-gateway:<version-tag>` |
| Executor | `ghcr.io/<owner>/<repository>-executor:<version-tag>` |

Registry names use the lowercase GitHub repository name. Releases contain:

- `install.yaml` for the controller, gateway, CRDs and RBAC.
- `images.json` with immutable image references and the source revision.
- `repository-task.yaml`, an editable workspace and Profile example.
- `gateway-client_linux_amd64.tar.gz`, `gateway-client_linux_arm64.tar.gz`, `gateway-client_darwin_amd64.tar.gz` and `gateway-client_darwin_arm64.tar.gz`, each containing the binary and license.
- `SHA256SUMS` covering all those assets.

`make build-cli-release` builds the four CLI archives into `dist/` without publishing. From source, `go build -o bin/gateway-client ./cmd/gateway-client` builds only the local client. Stable asset names support both `/releases/latest/download/…` and `/releases/download/<version>/…` URLs. Prereleases use their explicit version URLs.

Administrators install Agent Sandbox, provision PostgreSQL and Secrets, and approve their own versioned Sandbox templates/pools. The operator installation manifest does not apply the example automatically.

Enable GitHub Actions and grant its token package and release publication permissions. Anonymous installation requires a public repository and public visibility for all three GHCR packages. Protect release tags and require CI before merging. The workflow refuses to replace an existing release; publish a new version for corrections. Branch and pull-request runs do not publish releases.
