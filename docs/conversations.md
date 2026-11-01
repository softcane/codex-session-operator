# Conversations

Use the gateway to start a conversation from a Ready Profile, send follow-up messages, read results and request cleanup. The gateway saves input before a worker creates the Kubernetes Session. Its receipt provides a conversation ID and URL that remain usable during provisioning and after cleanup.

HTTP `202` means the input was durably accepted. To establish completion, read the new root turn and its output. A Ready Session, a completed subagent or an earlier completed turn does not establish the outcome of the latest message.

## Workflow tasks

Use a `WorkflowTemplate` for a coordinator that can delegate to separate Sessions. Discover templates, start a task and read its durable state with these routes. All use `/v1/namespaces/{namespace}`; every POST requires a stable `Idempotency-Key`.

| Endpoint | Request | Result |
| --- | --- | --- |
| `GET /workflows` | None. | Configured workflow summaries. |
| `GET /workflows/{name}` | Template name. | Identity, generation, digest and spec. |
| `POST /tasks` | `{"workflow":"go-bugfix","text":"Investigate this bug"}` | `202` committed receipt with `task_id`, `task_url` and `event_id`. |
| `GET /tasks/{id}` | Task ID. | Accepted configuration, members, events, saved results and lifecycle state. |
| `GET /tasks/{id}/results/{result_id}` | Exact saved result ID. | Result provenance, original JSON in `raw` and its `sha256`. |
| `POST /tasks/{id}/messages` | `{"text":"Follow-up question"}` | `202` committed follow-up acceptance. |
| `POST /tasks/{id}/stop` | `{}` | `202` committed stop acceptance; cleanup continues asynchronously. |

Discovery describes configuration; member acceptance still requires a current Ready Profile and dependencies. Task start freezes the WorkflowTemplate identity and content. Each assignment binds its Profile policy when accepted. Profile edits can affect later assignments in the same task, while accepted members retain their selected identities and policy.

```sh
gateway-client --origin "$ORIGIN" --namespace "$NAMESPACE" workflows show go-bugfix
gateway-client --origin "$ORIGIN" --namespace "$NAMESPACE" --state-dir "$STATE" \
  task start --workflow go-bugfix --text-file bug.txt --operation bugfix-1
gateway-client --origin "$ORIGIN" --namespace "$NAMESPACE" task read TASK_ID
gateway-client --origin "$ORIGIN" --namespace "$NAMESPACE" \
  task result TASK_ID RESULT_ID --output result.json
gateway-client --origin "$ORIGIN" --namespace "$NAMESPACE" --state-dir "$STATE" \
  task continue TASK_ID --text-file follow-up.txt --operation follow-up-1
gateway-client --origin "$ORIGIN" --namespace "$NAMESPACE" --state-dir "$STATE" \
  task stop TASK_ID --operation stop-1
gateway-client --origin "$ORIGIN" --namespace "$NAMESPACE" --state-dir "$STATE" resume bugfix-1
```

Substitute returned IDs for `TASK_ID` and `RESULT_ID`. `resume` replays the journaled operation with its original identity; it does not request a replacement member. Changed content under the same operation identity conflicts. `task result --output` saves the exact original result JSON bytes after checking their SHA-256. Without `--output`, the command displays the result envelope.

### Assignments and evidence

Starting authorizes the coordinator to choose from the configured roles. Roles and named outputs are application choices; the gateway does not require every role, a fixed sequence or a universal reviewer. Existing consequential external-tool approvals still apply. Each self-hosted member has its own Session, Sandbox and workspace; files do not implicitly cross workspaces.

The coordinator uses `workflow_delegate` with a role, assignment text and exact prerequisite result IDs. A role's dependencies require successful saved results from this task; the gateway never chooses a result by newest timestamp. A fresh delegation action may repeat a role. Transport replay preserves the existing assignment. Assignments are immutable, with no automatic replacement, retry or dependent rerun. Their complete initial input, including prerequisite result IDs, must fit the existing 256 KiB input boundary. Runtime input and relationships stay in PostgreSQL; Session instructions contain reusable configuration.

Members submit explicit results through `workflow_result`; `workflow_read_result` retrieves exact saved evidence. The dispatcher derives authority from verified Session/root-turn/call provenance, rather than model-supplied task or member IDs. Only each independent Session's main agent calls these function tools; native subagents do not support function tools, as described in the [provider multi-agent guide](https://developers.openai.com/api/docs/guides/agents-api/multi-agent#tools-available-to-subagents). The first committed terminal result is final for that assignment. Exact replay returns it; changed terminal content conflicts. Corrections require a new explicit assignment.

Result JSON supports `success`, `failed`, `cancelled` and `incomplete` outcomes. Successful results must include the role's configured outputs. Output labels are arbitrary; instructions specify their meaning. Patch outputs carry a full 40- or 64-character hexadecimal Git base revision, content digest, checks and limitations. Checks require an explicit exit code and must match saved completed or failed command observations; saved source snapshots preserve that evidence. A synthesis cites exact saved result IDs. Validation checks completeness and provenance, not whether an argument or scientific claim is true.

The entire original UTF-8 result JSON, including metadata, is limited to 256 KiB of encoded bytes. Invalid or oversized submissions leave earlier evidence intact and do not finalize the assignment. Save a smaller explicit incomplete result when the full evidence cannot fit; the workspace stays available until stop. Retrieval returns original JSON bytes through `raw` and their SHA-256, including patch content. The CLI retains its existing 8 MiB response limit and rejects oversized responses without truncating them. A saved result and durable handoff do not prove the coordinator processed it. Turn completion does not prove result submission, and input acceptance does not identify which provider turn processed the input.

### Stop and retained evidence

Tasks remain open after results and synthesis. Continue talking to the coordinator or explicitly request more assignments. Only `stop` permanently blocks fresh input, delivery and delegation, accounts for in-flight effects and requests closure of every known member, including the coordinator. Read the task for cleanup observations; `202` is not completed cleanup.

Each accepted Session keeps its Profile-derived workspace policy. `Delete` removes execution and workspace resources; retrieve evidence first, because stop can discard files never successfully transferred. `Retain` suspends execution, waits for Pod absence, detaches the Sandbox and deletes the claim while preserving files. Both preserve saved gateway evidence. Uncertain cleanup keeps claims and finalizers. Gateway restart recovers existing identities without starting replacement work.

The 16 MiB stored-record boundary still applies. Open task and member records reserve 2 KiB for cleanup; member submission logs also reserve one cleanup cancellation beyond the ordinary input limit. Capacity failures hold fresh work without erasing earlier evidence. WorkflowTemplate adds no team budget or task deadline; spending remains per Session. Task and member records are included in logical backups and protected from archive/purge, which are explicitly unsupported for these records. Restore holds block fresh task effects while allowing reads and exact replay.

The [Go bugfix](../examples/workflows/go-bugfix.yaml) and [research](../examples/workflows/research.yaml) files configure these choices. The full argument/experiment/review/synthesis research journey remains a later pilot target. Simulated-provider verification exercises transport and lifecycle behavior; it does not establish real-model collaboration, billing or production termination behavior.

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

For a runnable CLI example, follow [getting started](getting-started.md). This page is the CLI and HTTP reference.

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

See [PostgreSQL setup](deployment.md#postgresql) for the database connection Secret.

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

The CLI saves each operation to disk before sending it, then saves its receipt before reporting success. `resume` reuses the exact subject, namespace, target, body and idempotency key after lost responses or crashes. Journals contain input and receipts; protect and retain them. Journal paths must be canonical paths without symlink components, including workstation temporary-directory aliases. No gateway token file is required.

A mutation's exit 0 means durable acceptance. `watch ID --root-id ROOT_ID` reports the selected root's outcome. `watch ID --operation OPERATION_ID` observes changes, but the provider cannot reliably match each input to a root turn. Send one task at a time and inspect a new root for each follow-up.

| Exit code | Meaning |
| --- | --- |
| `0` | Mutation accepted, read succeeded, selected root completed, or watch observed a closed attempt. |
| `1` | Request rejected or another error occurred. |
| `2` | Invalid command or arguments. |
| `3` | Observation timed out while pending. |
| `4` | Acceptance is uncertain, or a root outcome was observed without input-to-turn correlation. |
| `5` | The selected root failed or was cancelled. |
| `130` | The process was interrupted before successful completion. |

Conversation detail includes `lifecycle.delivery_held`, `execution_ready`, `delivery_observation`, `reason`, `recovery` and `actions`. If delivery is held, inspect the Session's preparation, workspace and connection conditions. Accepted input stays saved while the executor is unavailable. Cancel and close remain available. Start fresh requires completed cleanup and creates a separate Session and workspace.
