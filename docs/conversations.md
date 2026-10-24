# Conversations

Use the gateway to start a conversation from a Ready Profile, send follow-up messages, read results and request cleanup. The gateway saves input before a worker creates the Kubernetes Session. Its receipt provides a conversation ID and URL that remain usable during provisioning and after cleanup.

HTTP `202` means the input was durably accepted. To establish completion, read the new root turn and its output. A Ready Session, a completed subagent or an earlier completed turn does not establish the outcome of the latest message.

## Profiles

A namespaced `Profile` CRD selects an Agent, Environment, optional Vaults and explicit spending policy. The HTTP request field `route_id` contains the Profile name. Fresh acceptance requires a current Ready Profile and Ready dependencies with matching identities; a same-name replacement cannot substitute a different resource after acceptance.

All routes below use the prefix `/v1/namespaces/{namespace}`.

| Method and route | Result |
| --- | --- |
| `GET /profiles?limit=100&after=CURSOR` | Profile page with `more` and `next_cursor`. |
| `GET /profiles/{name}` | One Profile's readiness, safe resource names, UID/generation/version metadata and spending policy. |
| `GET /identity` | Stable `caller_subject` and URL-selected namespace, without Kubernetes reads. The CLI uses this for replay during observation outages. |
| `GET /catalog` | Catalog including a bounded first page of Profiles. Use `/profiles` to read subsequent pages. |

The gateway makes bounded authoritative Kubernetes reads, so additions, edits, deletions and readiness changes need no restart.

At acceptance the gateway freezes the Profile identity, policy and selected dependency UIDs. Its worker and the controller compare resolved dependencies before remote creation. Changes apply to new conversations and explicit fresh attempts. Reads and exact replay remain available after a Profile is changed, retired or deleted.

## Start a conversation with the CLI

This example assumes a reachable gateway and a Ready `my-go-project` Profile from the [repository walkthrough](my-go-project.md). It uses whichever provider your deployment configures; the walkthrough uses real OpenAI and incurs charges. For a no-key example, use the [retained demo](development.md#use-the-retained-demo).

This example also requires `jq`. Build the client with `make build`. From the operator checkout, create a private directory for task text, receipts and the operation journal. Set the gateway origin and namespace to match your deployment; the origin is an HTTP or HTTPS origin without a path.

```sh
umask 077
mkdir -p .private/conversations
export CONVERSATION_DIR="$(cd .private/conversations && pwd -P)"
export GATEWAY_ORIGIN=http://127.0.0.1:18082
export NAMESPACE=codex-session-operator-system

client() {
  ./bin/gateway-client --origin "$GATEWAY_ORIGIN" --namespace "$NAMESPACE" \
    --state-dir "$CONVERSATION_DIR/operations" "$@"
}
client profiles show my-go-project

cat > "$CONVERSATION_DIR/task.txt" <<'TASK'
Investigate why retries with the same request_id create duplicate jobs.
Read AGENTS.md, reproduce the behavior and report the cause and test evidence.
TASK
client start --profile my-go-project --text-file "$CONVERSATION_DIR/task.txt" \
  --operation investigate-retries > "$CONVERSATION_DIR/receipt.json"
CONVERSATION_ID="$(jq -er .receipt.conversation_id "$CONVERSATION_DIR/receipt.json")"
client read "$CONVERSATION_ID" > "$CONVERSATION_DIR/conversation.json"
jq '[.history.turns[]? | select(.subagent_id == null) | {id, status}]' \
  "$CONVERSATION_DIR/conversation.json"
```

`start` returns a receipt after acceptance. History may still be empty while the Session is provisioned. Repeat `client read` and inspect the root turns until a root appears. For this first task, with no concurrent input, select that root and watch its outcome:

```sh
ROOT_ID="$(jq -er '[.history.turns[]? | select(.subagent_id == null)] | last | .id // error("No root turn observed yet")' \
  "$CONVERSATION_DIR/conversation.json")"
client watch "$CONVERSATION_ID" --root-id "$ROOT_ID" --timeout 15m
client read "$CONVERSATION_ID" > "$CONVERSATION_DIR/conversation.json"
```

A completed root means that execution finished. Read the saved assistant output and command results to assess success. The provider does not reliably correlate each accepted message with a root turn, so this example sends one task at a time. The [repository walkthrough](my-go-project.md#8-observe-the-actual-outcome) shows how to detect a new root after a follow-up.

After a lost response or process crash, resume the saved operation with the same origin, namespace, service replay identity and journal:

```sh
client resume investigate-retries
```

Use a new operation name for each new message or lifecycle request. To close this conversation:

```sh
client close "$CONVERSATION_ID" --operation close-investigation
client read "$CONVERSATION_ID"
```

Close acceptance starts background cleanup. Wait for `summary.state` to be `closed` and `summary.cleaned` to be `true` before starting fresh. Saved history remains readable at the original conversation URL. A restart returns a new conversation ID and URL.

## HTTP API

All routes in this section use the prefix `/v1/namespaces/{namespace}`. Every mutation in the table requires an `Idempotency-Key` header.

The gateway does not authenticate callers or enforce user roles. Your deployment layer manages access control. Requests select a Kubernetes namespace in the URL, and all callers reaching those routes share access to its conversations. `caller_subject` is a stable service replay identifier, not a user identity.

| Endpoint | Request | Result |
| --- | --- | --- |
| `POST /conversations` | `Idempotency-Key` and `{"route_id":"profile","text":"task","title":"optional"}` | `202` receipt and `Location`. |
| `GET /conversations` | Optional `limit` and `after`. | Attempt list with `more` and `next_cursor`. |
| `GET /conversations/{id}` | Conversation ID. | Attempt summary, accepted input, history, actions and recovery indicators. |
| `POST /conversations/{id}/messages` | `Idempotency-Key` and `{"text":"evidence"}`. | `202` after commit. |
| `POST /conversations/{id}/close` | `Idempotency-Key` and `{}`. | `202`; cleanup proceeds in the background. |
| `POST /conversations/{id}/restart` | `Idempotency-Key` and `{"text":"fresh task"}`. | `202` with a new ID after earlier cleanup. |

Listings default to 100 attempts and accept limits from 1 to 100. Pass `next_cursor` as the next request's `after` value.

```json
{
  "conversation_id": "conv-<opaque-identity>",
  "conversation_url": "/v1/namespaces/team/conversations/conv-<opaque-identity>",
  "route_id": "approved-profile",
  "attempt": 1,
  "event_id": "<durable-event-identity>",
  "received_at": "2026-10-13T12:00:00Z",
  "acceptance": "committed"
}
```

### Request limits

| Field | Requirement |
| --- | --- |
| `route_id` | Profile name for a new conversation. |
| `text` | Nonempty text, at most 256 KiB. |
| `title` | Optional on creation; at most 256 UTF-8 bytes, without control characters. |
| `Idempotency-Key` | 1–128 ASCII letters, numbers, dots, underscores, colons or hyphens. The `action-` prefix is reserved. |

The decoder rejects unknown fields, invalid UTF-8, trailing JSON and oversized bodies. CRLF text normalizes to LF for replay comparison.

## Retries and lifecycle

Retain each operation's key, target URL and body until its outcome is known. A transport error does not prove rejection. Retry the same request or inspect the saved conversation; a new key denotes a new operation.

Creation keys are scoped to namespace and stable service replay subject. Reusing a key with a different Profile, text or title returns `409`. Exact replay returns the original receipt after closure, Profile deletion or gateway replacement.

Later keys are scoped to the service replay subject and conversation lineage. Separate conversations may reuse a key. Changing the operation, target attempt or body within the same lineage conflicts. Replaying an old URL after restart returns the original receipt rather than applying input to the new attempt.

Fresh input to a closed or superseded attempt returns `410`. Restart requires the latest attempt to be closed and all previous attempts to have completed cleanup. It uses the current selected Profile; retiring it prevents restart but leaves reads and exact replay available. A lineage supports at most 16 attempts.

| Status | Meaning |
| --- | --- |
| `202` | Request committed; observe the returned conversation. |
| `400` | Invalid request, content or key. |
| `404` | Conversation, profile or namespace path unavailable. |
| `409` | Changed replay content, premature restart or lifecycle limit. |
| `410` | Attempt cannot accept fresh input. |
| `503` | Storage, observation, capacity or restore admission unavailable. Retain the key and body. |

### Read lifecycle and completion

`summary.state` means `open`, `closing` or `closed`. `summary.active` identifies the latest attempt, not whether inference is running. `summary.phase` is a gateway display summary derived from current Session conditions; the Session CRD has no phase field. Use current-generation conditions for readiness, allow unknown condition types/reasons, and do not infer turn completion from the display summary. Read root turns in `history.turns` to establish execution outcomes; a completed subagent turn does not complete its parent task.

The receipt's `event_id` identifies gateway acceptance, not an OpenAI turn. GET reads the saved history snapshot, which can lag the provider. After a completed first turn, check the new root turn when sending a follow-up; an earlier completed turn alone does not establish the outcome of that follow-up.

### Approvals, cancellation and events

Use the live Session URL in `summary.session_url` while that Session exists:

| Method and suffix | Request or result |
| --- | --- |
| `GET /actions` | Read saved actions and pending approvals. |
| `POST /actions/{id}` | Save `{"decision":"approve"}` or `{"decision":"deny"}` for the action. |
| `POST /cancel` | Queue cancellation with `{"turn_id":"ROOT_ID"}` and an `Idempotency-Key`. |
| `GET /events` | Read the live SSE stream. |

Append these suffixes to `summary.session_url`, rather than to the conversation URL. Saved conversation reads remain available after Session cleanup.

## Storage and recovery

PostgreSQL retains IDs, replay receipts, inputs and attempt mappings. Restore holds block fresh mutations until an administrator audits recovery; reads and exact replay remain available. Archive removes payloads from the selected closed record, including saved history, input text and action arguments or results. Receipts, replay fingerprints and lifecycle identities remain. Conversation and Session records have separate archive operations; archive each record whose content should be removed. See [deployment](deployment.md#storage-and-recovery).

Gateway replicas initialize a fresh database from the embedded schema while holding a PostgreSQL advisory lock. Subsequent replicas and restarts use the existing tables. See [deployment](deployment.md#postgresql-connection-and-schema) for the required gateway database role.

Clients must retain uncertain operation keys, target URLs and input across retries and process restarts. Creation keys are bound to the original `caller_subject`; retain the same configured service replay identity.

## CLI and execution readiness

Supply `--origin`, `--namespace` and, for mutations, a private `--state-dir`. Use `--ca-file` when HTTPS requires a custom CA. Global flags precede the command; mutation flags follow the conversation ID when one is required. `--text-file -` reads stdin.

| Command | Purpose |
| --- | --- |
| `profiles list` / `profiles show NAME` | Discover approved Profiles and inspect readiness. |
| `list` / `read ID` | List attempts or read one attempt and its saved history. |
| `start --profile NAME --text-file FILE` | Start a conversation. |
| `continue ID --text-file FILE` | Send a follow-up to the same attempt. |
| `watch ID --root-id ROOT_ID` | Observe a selected provider root's terminal outcome. |
| `watch ID --operation OPERATION_ID` | Observe changes since an operation's saved baseline, without claiming input-to-turn correlation. |
| `close ID` | Stop new input and request cleanup. |
| `start-fresh ID --text-file FILE` | Create a new attempt after cleanup. |
| `resume OPERATION_ID` | Replay an immutable saved operation. |
| `operations` | List saved local operation identities. |

Give each mutation a stable local name with `--operation ID`. Run `bin/gateway-client --help` for all options.

The CLI fsyncs a private immutable operation before POST and its receipt before successful return. `resume` reuses the exact subject, namespace, target, body and idempotency key after lost responses or crashes. Journals contain input and receipts; protect and retain them. Journal paths must be canonical paths without symlink components, including workstation temporary-directory aliases. No gateway token file is required.

A mutation's exit 0 means durable acceptance. Watching an operation cannot establish per-message completion because the provider does not return a reliable input-to-root correlation. The CLI distinguishes new observed roots, pending timeouts and ambiguous outcomes; a prior completed or failed root, subagent or concurrent steering cannot prove that input finished. `watch ID --root-id ROOT_ID` reports the selected provider root's terminal outcome.

| Exit code | Meaning |
| --- | --- |
| `0` | Mutation accepted, read succeeded, selected root completed, or watch observed a closed attempt. |
| `1` | Request rejected or another error occurred. |
| `2` | Invalid command or arguments. |
| `3` | Observation timed out while pending. |
| `4` | Acceptance is uncertain, or a root outcome was observed without input-to-turn correlation. |
| `5` | The selected root failed or was cancelled. |
| `130` | The process was interrupted before successful completion. |

Conversation detail includes `lifecycle.delivery_held`, `execution_ready`, `delivery_observation`, `reason`, `recovery` and `actions`. `execution_ready` reports current owned compute and provider connection state. `delivery_observation` distinguishes a runtime observation from an inspected local worker stream; a follower replica does not claim it inspected another process’s stream. The elected worker applies its own live stream veto and current runtime checks before and after claiming each delivery. A preparing, failed-init, missing/replaced or disconnected executor holds delivery while accepted input remains durable. Pod preparation proof is tied to the current Session-owned SandboxClaim, Sandbox, Pod, workspace and frozen configuration. Cancel and close remain available while delivery is held. Start fresh requires completed cleanup and produces independent remote identity and workspace.
