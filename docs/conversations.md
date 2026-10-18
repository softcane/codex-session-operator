# Conversations

Choose an approved profile and send a first message. The gateway commits the request before a worker creates its Kubernetes Session. The receipt gives you a conversation ID and URL that work during provisioning and after cleanup. HTTP `202` confirms acceptance; saved provider turns establish completion.

## Profiles

A namespaced `Profile` CRD selects an Agent, Environment, optional Vaults and explicit spending policy. Incoming requests retain the `route_id` field, whose value is the Profile name. Fresh acceptance requires a current Ready Profile and Ready dependencies with matching identities; a same-name replacement cannot substitute a different resource after acceptance.

`GET /v1/namespaces/{namespace}/profiles?limit=100&after=CURSOR` lists safe selections with `more` and `next_cursor`. `GET /v1/namespaces/{namespace}/profiles/{name}` reads one. These responses include `caller_subject`, readiness, safe resource names, UID/generation/version metadata and spending policy. Catalog includes a bounded first Profile page; use `/profiles` for subsequent pages. `GET /v1/namespaces/{namespace}/identity` returns the authenticated caller subject and namespace without Kubernetes reads, allowing CLI replay during resource-observation outages.

The gateway makes bounded authoritative Kubernetes reads, so additions, edits, deletions and readiness changes need no restart.

At acceptance the gateway freezes the Profile identity, policy and selected dependency UIDs. Its worker and the controller compare resolved dependencies before remote creation. Changes apply to new conversations and explicit fresh attempts. Reads and exact replay remain available after a Profile is changed, retired or deleted.

## Real conversation walkthrough

Use the [my-go-project walkthrough](my-go-project.md) for a self-hosted repository task with Profile CRDs, registered skills and the Go CLI. Use `conversation_url` from the original receipt and retain each operation's key and body. Restart returns a new URL; the old attempt remains readable and rejects fresh messages.

## HTTP API

Requests use a namespace-scoped bearer token. Authorized callers in the same namespace can read and interact with its conversations; creator attribution does not establish per-user isolation.

| Endpoint | Request | Result |
| --- | --- | --- |
| `POST /v1/namespaces/{namespace}/conversations` | `Idempotency-Key` and `{"route_id":"profile","text":"task","title":"optional"}` | `202` receipt and `Location`. |
| `GET /v1/namespaces/{namespace}/conversations` | Optional `limit` and `after`. | Attempt list with `more` and `next_cursor`. |
| `GET /v1/namespaces/{namespace}/conversations/{id}` | Conversation ID. | Attempt summary, accepted input, history, actions and recovery indicators. |
| `POST /v1/namespaces/{namespace}/conversations/{id}/messages` | `Idempotency-Key` and `{"text":"evidence"}`. | `202` after commit. |
| `POST /v1/namespaces/{namespace}/conversations/{id}/close` | `Idempotency-Key` and `{}`. | `202`; cleanup proceeds in the background. |
| `POST /v1/namespaces/{namespace}/conversations/{id}/restart` | `Idempotency-Key` and `{"text":"fresh task"}`. | `202` with a new ID after earlier cleanup. |

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

Message text must be nonempty and at most 256 KiB. Titles allow at most 256 UTF-8 bytes and no control characters. Keys use 1–128 ASCII letters, numbers, dots, underscores, colons or hyphens; the `action-` prefix is reserved. The decoder rejects unknown fields, invalid UTF-8, trailing JSON and oversized bodies. CRLF text normalizes to LF for replay comparison.

## Retries and lifecycle

Retain each operation's key, target URL and body until its outcome is known. A transport error does not prove rejection. Retry the same request or inspect the saved conversation; a new key denotes a new operation.

Creation keys are scoped to namespace and authenticated subject. Reusing a key with a different Profile, text or title returns `409`. Exact replay returns the original receipt after closure, Profile deletion or gateway replacement.

Later keys are scoped to the caller and conversation lineage. Separate conversations may reuse a key. Changing the operation, target attempt or body within the same lineage conflicts. Replaying an old URL after restart returns the original receipt rather than applying input to the new attempt.

Fresh input to a closed or superseded attempt returns `410`. Restart requires the latest attempt to be closed and all previous attempts to have completed cleanup. It uses the current selected Profile; retiring it prevents restart but leaves reads and exact replay available. A lineage supports at most 16 attempts.

| Status | Meaning |
| --- | --- |
| `202` | Request committed; observe the returned conversation. |
| `400` | Invalid request, content or key. |
| `401` | Authentication failed. |
| `404` | Conversation, profile or authorized namespace path unavailable. |
| `409` | Changed replay content, premature restart or lifecycle limit. |
| `410` | Attempt cannot accept fresh input. |
| `503` | Storage, observation, capacity or restore admission unavailable. Retain the key and body. |

`summary.state` means `open`, `closing` or `closed`. `summary.active` identifies the latest attempt, not whether inference is running. `summary.phase` describes its Kubernetes Session. Read root turns in `history.turns` to establish execution outcomes; a completed subagent turn does not complete its parent task.

The receipt's `event_id` identifies gateway acceptance, not an OpenAI turn. GET reads the saved history snapshot, which can lag the provider. After a completed first turn, check the new root turn when sending a follow-up; an earlier completed turn alone does not establish the outcome of that follow-up.

Approval, cancellation and SSE routes use the live Session URL returned in `summary.session_url`. The conversation API has no `/actions`, `/cancel` or `/events` subroutes. Saved conversation reads remain available after Session cleanup.

## Storage and recovery

PostgreSQL retains IDs, replay receipts, inputs and attempt mappings. Restore holds block fresh mutations until an administrator audits recovery; reads and exact replay remain available. Archive removes payloads from the selected closed record, including saved history, input text and action arguments or results. Receipts, replay fingerprints and lifecycle identities remain. Conversation and Session records have separate archive operations; archive each record whose content should be removed. See [deployment](deployment.md#storage-and-recovery).

Gateway replicas initialize a fresh database from the embedded schema while holding a PostgreSQL advisory lock. Subsequent replicas and restarts use the existing tables. See [deployment](deployment.md#postgresql-connection-and-schema) for the required gateway database role.

Clients must retain uncertain operation keys, target URLs and input across retries and process restarts. Creation keys are bound to the original `caller_subject`; retry with that caller's credentials.

## CLI and execution readiness

Build `bin/gateway-client` with `make build`. Supply explicit `--origin`, `--namespace`, `--token-file`, `--ca-file` when needed, and a private `--state-dir`; globals precede the command. Commands are `profiles list`, `profiles show NAME`, `list`, `start --profile NAME --text-file FILE`, `read ID`, `watch ID`, `continue ID --text-file FILE`, `close ID`, `start-fresh ID --text-file FILE`, `resume OPERATION_ID` and `operations`. Use `--operation ID` to give a mutation a stable local identity. Text `-` reads stdin.

The CLI fsyncs a private immutable operation before POST and its receipt before successful return. `resume` reuses the exact subject, namespace, target, body and idempotency key after lost responses or crashes. Journals contain input and receipts; protect and retain them. Token and journal paths must be canonical paths without symlink components, including workstation temporary-directory aliases. Credentials are loaded from restricted files and do not enter the journal.

A mutation's exit 0 means durable acceptance. Watching an operation cannot establish per-message completion because the provider does not return a reliable input-to-root correlation. The CLI distinguishes new observed roots, pending timeouts and ambiguous outcomes; a prior completed or failed root, subagent or concurrent steering cannot prove that input finished. `watch ID --root-id ROOT_ID` reports the selected provider root's terminal outcome. Run `--help` for the exit codes.

Conversation detail includes `lifecycle.delivery_held`, `execution_ready`, `delivery_observation`, `reason`, `recovery` and `actions`. `execution_ready` reports current owned compute and provider connection state. `delivery_observation` distinguishes a runtime observation from an inspected local worker stream; a follower replica does not claim it inspected another process’s stream. The elected worker applies its own live stream veto and current runtime checks before and after claiming each delivery. A preparing, failed-init, missing/replaced or disconnected executor holds delivery while accepted input remains durable. Pod preparation proof is tied to the current Pod, workspace claim and frozen configuration. Cancel and close remain available while delivery is held. Start fresh requires completed cleanup and produces independent remote identity and workspace.
