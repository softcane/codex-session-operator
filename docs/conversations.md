# Conversations

Choose an approved profile and send a first message. The gateway commits the request before a worker creates its Kubernetes Session. The receipt gives you a conversation ID and URL that work during provisioning and after cleanup. HTTP `202` confirms acceptance; saved provider turns establish completion.

## Profiles

A profile is a trusted `WorkRoute`. Its configuration selects a namespace, Agent, Environment, optional Vaults and spending policy. Incoming requests supply `route_id`, message text and an optional title.

`GET /v1/namespaces/{namespace}/catalog` lists profiles with their resource names, spend policy and readiness. It also returns `caller_subject`, the authenticated identity that scopes creation keys. It exposes no credential values or Agent instructions.

The gateway freezes the selected route when it accepts the first message. The controller later freezes resolved dependency identities and creation settings. A changed route applies to new conversations and restarted attempts. Route files reload when the gateway restarts.

## Local demo

Run `make demo`, source the `env.sh` printed by the runner and start its printed gateway port-forward. The demo uses a simulated provider and makes no real OpenAI inference calls.

```sh
python3 -B test/e2e/client.py conversation demo-investigation \
  --key checkout-start-001 --title 'Checkout investigation' \
  --text 'Explain the supplied checkout evidence.' \
  --output "$DEMO_DIR/conversation-receipt.json"
```

Copy `conversation_url` from the receipt into these commands:

```sh
python3 -B test/e2e/client.py get '<conversation_url>' --wait-for completed --turns 1
python3 -B test/e2e/client.py message '<conversation_url>' \
  --key checkout-evidence-002 --text 'Here is additional evidence.'
python3 -B test/e2e/client.py close '<conversation_url>' --key checkout-close-003
python3 -B test/e2e/client.py get '<conversation_url>' --wait-for closed
python3 -B test/e2e/client.py restart '<conversation_url>' \
  --key checkout-restart-004 --text 'Start a fresh investigation.'
```

Restart returns a new ID and URL. The previous attempt remains readable and rejects fresh messages. The helper waits for cleanup before reporting closed.

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

Creation keys are scoped to namespace and authenticated subject. Reusing a key with a different profile, text or title returns `409`. Exact replay returns the original receipt after closure, route retirement or gateway replacement.

Later keys are scoped to the caller and conversation lineage. Separate conversations may reuse a key. Changing the operation, target attempt or body within the same lineage conflicts. Replaying an old URL after restart returns the original receipt rather than applying input to the new attempt.

Fresh input to a closed or superseded attempt returns `410`. Restart requires the latest attempt to be closed and all previous attempts to have completed cleanup. It uses the current route; retiring that route prevents restart but leaves reads and exact replay available. A lineage supports at most 16 attempts.

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

## Storage and upgrades

PostgreSQL retains IDs, replay receipts, inputs and attempt mappings. Restore holds block fresh mutations until an administrator audits recovery; reads and exact replay remain available. Archive clears saved content and titles while retaining receipt identities and archive indicators. See [deployment](deployment.md#storage-and-recovery).

Conversation source records use version 2. Legacy Work and direct Session records retain version 1. Complete a coordinated gateway rollout before using this API: older gateways reject version 2, and an older elected worker can delay delivery. Older binaries cannot restore backups containing version 2 records.

The browser keeps uncertain keys and bodies in memory across retry and reconnect, bound to the original `caller_subject`. Reloading the page discards that state. It stores no tokens or task text in browser persistence; clients needing recovery across reloads must retain their own keys and input.
