# Development

Use Go matching `go.mod` and Make for local checks. Node.js runs the simulated provider contract tests. The end-to-end suite also needs Docker, Python 3.10 or newer, Kind, `kubectl` and OpenSSL. Make installs pinned Kustomize, controller-gen, envtest and lint tools into `bin/` when their targets need them.

## Choose checks for your change

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

`make build` and `make test` also regenerate manifests and deepcopy code, format Go source and run vet. Review generated changes before committing. Make downloads pinned tools as needed; see [Makefile](../Makefile) for the target dependencies.

Run the PostgreSQL gate for transaction or recovery changes. Run the isolated end-to-end suite for changes to deployed Pod, PVC or network behavior. The [quality workflow](../.github/workflows/quality.yml) runs the Go, PostgreSQL, lint, vulnerability and simulated end-to-end gates in CI.

## Use the retained demo

With the end-to-end prerequisites installed and Docker running, run:

```sh
make demo
```

The demo runs the same simulated-provider suite as `make test-e2e`, then retains its successful Kind deployment. It makes no model calls and needs no OpenAI key. The runner prints a private report path, an `env.sh` path, a port-forward command and the command to delete its cluster. Ordinary `make test-e2e` runs remove their cluster.

Source the printed `env.sh` and run the printed port-forward in one terminal. In another terminal, return to this checkout and source the same file. With `jq` installed, you can submit a separate demo conversation:

```sh
# First source the env.sh file printed by make demo in this terminal.
export DEMO_DIR="$(cd "$DEMO_DIR" && pwd -P)"
client() {
  ./bin/gateway-client --origin "$DEMO_URL" --namespace "$DEMO_NAMESPACE" \
    --state-dir "$DEMO_DIR/manual-operations" "$@"
}
client profiles show demo-investigation
printf '%s\n' 'Explain the supplied demo evidence.' > "$DEMO_DIR/manual-task.txt"
client start --profile demo-investigation --text-file "$DEMO_DIR/manual-task.txt" \
  --operation demo-investigation-1 > "$DEMO_DIR/manual-receipt.json"
CONVERSATION_ID="$(jq -er .receipt.conversation_id "$DEMO_DIR/manual-receipt.json")"
client read "$CONVERSATION_ID"
```

The receipt confirms saved input. Repeated reads show the simulated turn history. Responses are fixed test data; they do not demonstrate real inference or repository work. Use a new operation name for new input and `client resume demo-investigation-1` to retry the saved request. See [conversations](conversations.md#cli-and-execution-readiness) for root observation and exit codes.

When finished, run the exact cluster-deletion command printed by the demo. This removes its local Kubernetes resources, including its database and workspace volumes. Preserve any report or local caller journal you need first.

## Editing

Edit API source in `api/v1alpha1/`, then regenerate:

```sh
make manifests generate
```

Do not edit generated CRDs, RBAC or deepcopy code by hand. Keep Kubebuilder markers and `PROJECT` intact. Update the resource samples and public documentation when an API contract changes.

Match verification to the boundary you change: controller unit/envtest for reconciliation, provider wire tests for request mapping, PostgreSQL tests for durability, and the isolated end-to-end suite for deployed behavior. Use explicit Kubernetes contexts for manual work.

Keep prompts, transcripts, approvals, receipts and replay keys in the gateway store. CR status contains remote identities, conditions and safe snapshots. Before a remote create, persist its claim and recover ambiguous results by verified ownership. Do not bypass uncertainty by clearing claims or finalizers.

Self-hosted SandboxClaims belong to the Session UID; verify the complete claim, Sandbox and Pod ownership chain. Validate approved pools and template Secret references. Preserve `Delete` and `Retain` behavior, and stop fresh gateway input before cleanup.

## Images and release

The root [Dockerfile](../Dockerfile) provides separate `controller`, `gateway`, and shared `executor` production targets, plus isolated test fixtures. Build the production images with:

```sh
make docker-build-controller CONTROLLER_IMG=codex-controller:local
make docker-build-gateway GATEWAY_IMG=codex-gateway:local
make docker-build-executor EXECUTOR_IMG=codex-executor:local
```

The controller image contains `/manager`. The gateway image contains `/gateway`, `/gateway-admin`, and `/gateway-client`. The shared executor intentionally omits project toolchains. `make docker-build-application-executor` layers the `my-go-project` Go, Make, compiler, and GitHub CLI tools onto it; skills use a separate capability image.

Review the [release workflow](../.github/workflows/release.yml) before creating a version tag such as `v0.1.0` or `v0.1.0-rc.1`. It runs the quality gates for that tag, then publishes controller, gateway, and executor images to the repository's GitHub Container Registry namespace for `linux/amd64` and `linux/arm64`. Image tags preserve the full version tag; the workflow does not publish `latest`.

The GitHub Release includes `install.yaml` for the combined controller and gateway resources, `images.json` with immutable image references and source revision, and `SHA256SUMS`. PostgreSQL and credential provisioning remain deployment responsibilities. Keep local plans, credentials, reports and experimental harnesses in the ignored `.private/` directory.

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
