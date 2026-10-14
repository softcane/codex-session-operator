"use strict";

const byID = (id) => document.getElementById(id);
let token = "";
let namespace = "";
let callerSubject = "";
let selected = "";
let selectedUID = "";
let selectedWork = "";
let viewedWork = "";
let selectedConversation = "";
let conversationSummary;
let conversationCursor = "";
let conversationNextCursor = "";
let conversationPageVersion = 0;
let profiles = [];
const pendingConversations = new Map();
const pendingConversationActions = new Map();
let workConfigured = false;
let pendingTask;
const pendingClosures = new Map();
const pendingMessages = new Map();
let messageContextVersion = 0;
let selectionVersion = 0;
let pollTimer;

function error(message) { byID("error").textContent = message || ""; }
function apiPath(suffix) { return `/v1/namespaces/${encodeURIComponent(namespace)}${suffix}`; }
async function request(path, options = {}) {
  const headers = { Authorization: `Bearer ${token}`, ...(options.headers || {}) };
  const response = await fetch(path, { ...options, headers, cache: "no-store" });
  if (!response.ok) {
    const cause = new Error(`${response.status}: ${(await response.text()).trim()}`);
    cause.status = response.status;
    throw cause;
  }
  return response.json();
}
function conversationPath(id = selectedConversation) { return apiPath(`/conversations/${encodeURIComponent(id)}`); }
function messagePath() { return selectedConversation ? conversationPath() : apiPath(`/sessions/${encodeURIComponent(selected)}`); }
function messageIdentity() { return selectedConversation ? messagePath() : `${messagePath()}:${selectedUID}`; }
function updateMessageControls() {
  const pending = (selected || selectedConversation) && pendingMessages.get(messageIdentity());
  const writable = selectedConversation ? pending || (conversationSummary?.active && conversationSummary?.state === "open") : selected;
  byID("send").disabled = !token || !writable || Boolean(pending?.sending) || Boolean(pending && pending.subject !== callerSubject);
  byID("message").disabled = Boolean(pending);
  byID("send").textContent = pending ? "Retry saved message" : "Send message";
}
function resetMessageContext() {
  messageContextVersion++;
  for (const [identity, pending] of pendingMessages) if (!pending.conversation) pendingMessages.delete(identity);
  byID("message").value = "";
  clear(byID("conversation"));
  clear(byID("actions"));
  byID("selected-name").textContent = "";
  byID("session-state").textContent = "";
  byID("message-status").textContent = "";
  byID("close-conversation").hidden = true;
  byID("restart-form").hidden = true;
  updateMessageControls();
}
function clear(node) { node.replaceChildren(); }
function option(select, name, label) {
  const item = document.createElement("option");
  item.value = name;
  item.textContent = label;
  select.append(item);
}
async function refresh() {
  const contextVersion = messageContextVersion;
  const [catalog, result] = await Promise.all([
    request(apiPath("/catalog")), request(apiPath("/sessions")),
  ]);
  if (contextVersion !== messageContextVersion) return;
  callerSubject = catalog.caller_subject || "";
  for (const [id, entries, decorate] of [
    ["agent", catalog.agents, (v) => v.name],
    ["environment", catalog.environments, (v) => `${v.name} (${v.mode})`],
  ]) {
    const select = byID(id);
    const previous = select.value;
    clear(select);
    option(select, "", "Select a ready resource");
    for (const entry of entries || []) if (entry.ready) option(select, entry.name, decorate(entry));
    select.value = previous;
  }
  const routes = catalog.work_routes || [];
  profiles = routes;
  const profileSelect = byID("profile");
  const previousProfile = pendingConversations.get(namespace)?.input.route_id || profileSelect.value;
  clear(profileSelect);
  option(profileSelect, "", "Select an approved profile");
  for (const route of routes) option(profileSelect, route.id, `${route.id} — ${route.agent} / ${route.environment}${route.ready ? "" : " (preparing)"}`);
  if (pendingConversations.has(namespace) && !routes.some(route => route.id === previousProfile)) option(profileSelect, previousProfile, `${previousProfile} (pending request)`);
  profileSelect.value = previousProfile;
  updateConversationControls();
  workConfigured = routes.length > 0;
  byID("tasks-section").hidden = !workConfigured;
  byID("task-form").hidden = !workConfigured;
  byID("tasks-title").textContent = workConfigured ? "Start task" : "Work history";
  byID("create-title").textContent = workConfigured ? "Create Session directly (advanced)" : "Create Session";
  const routeSelect = byID("work-route");
  const previousRoute = routeSelect.value;
  clear(routeSelect);
  option(routeSelect, "", "Select an approved route");
  for (const route of routes) {
    option(routeSelect, route.id, `${route.id} — ${route.agent} / ${route.environment}${route.ready ? "" : " (preparing)"}`);
  }
  routeSelect.value = previousRoute;
  byID("start-task").disabled = !workConfigured;
  const list = byID("sessions");
  clear(list);
  for (const session of result.sessions || []) {
    const li = document.createElement("li");
    const button = document.createElement("button");
    button.textContent = "Open";
    button.addEventListener("click", () => selectSession(session.name, session.uid));
    li.append(button, document.createTextNode(`${session.name} — ${session.phase || "Pending"} — ${session.agent} / ${session.environment}`));
    list.append(li);
  }
  await refreshWork();
  await refreshConversations();
  if (contextVersion !== messageContextVersion) return;
  if (catalog.more || result.more) error("More resources or approved profiles exist than this catalog displays. Ask an administrator about omitted profiles; kubectl can inspect the full resource lists.");
}
function updateConversationControls() {
  const pending = pendingConversations.get(namespace);
  const sameCaller = !pending || pending.subject === callerSubject;
  if (pending && sameCaller) {
    byID("profile").value = pending.input.route_id;
    byID("first-message").value = pending.input.text;
    byID("conversation-label").value = pending.input.title || "";
  }
  if (pending && !sameCaller) byID("conversation-status").textContent = "A request from another caller remains unresolved. Reconnect as the original caller to retry, or inspect saved conversations.";
  for (const id of ["profile", "first-message", "conversation-label"]) byID(id).disabled = Boolean(pending);
  const profile = profiles.find(route => route.id === byID("profile").value);
  const spend = profile?.spend_control;
  byID("profile-details").textContent = profile ? `Agent: ${profile.agent} · Environment: ${profile.environment}${spend ? ` · Spend: ${spend.mode === "limited" ? `limited to ${spend.limitCents} US cents` : spend.mode}` : ""}${profile.ready ? "" : " · Dependencies preparing"}` : "Select a profile to see its Agent and Environment.";
  byID("start-conversation").disabled = !token || !sameCaller || Boolean(pending?.sending) || (!pending && (!profile || !profile.ready));
  byID("start-conversation").textContent = pending ? "Retry original request" : "Start conversation";
}
async function refreshConversations() {
  if (!token) return;
  const contextVersion = messageContextVersion;
  const pageVersion = conversationPageVersion;
  const cursor = conversationCursor;
  const result = await request(apiPath(`/conversations${cursor ? `?after=${encodeURIComponent(cursor)}` : ""}`));
  if (contextVersion !== messageContextVersion || pageVersion !== conversationPageVersion) return;
  conversationNextCursor = result.more && typeof result.next_cursor === "string" ? result.next_cursor : "";
  byID("conversation-first-page").disabled = !cursor;
  byID("conversation-next-page").disabled = !conversationNextCursor;
  byID("conversation-page-status").textContent = `${cursor ? "Later page" : "First page"} — ${result.more ? "more saved conversations available" : "end of saved conversations"}.`;
  const list = byID("conversations");
  clear(list);
  for (const summary of result.conversations || []) {
    const li = document.createElement("li");
    const open = document.createElement("button");
    open.type = "button";
    open.textContent = "Open conversation";
    open.addEventListener("click", () => selectConversation(summary.conversation_id).catch(cause => error(cause.message)));
    li.append(open, document.createTextNode(`${summary.title || summary.conversation_id} — ${summary.route_id} — attempt ${summary.attempt} — ${summary.state}${summary.phase ? ` — ${summary.phase}` : ""}${summary.archived ? " — earlier content archived" : ""}`));
    if (summary.last_error) renderItem(li, summary.last_error, "item work-error");
    list.append(li);
  }
}
async function changeConversationPage(cursor) {
  conversationCursor = cursor;
  conversationNextCursor = "";
  conversationPageVersion++;
  byID("conversation-next-page").disabled = true;
  byID("conversation-first-page").disabled = !cursor;
  byID("conversation-page-status").textContent = "Loading saved conversations…";
  await refreshConversations();
}
function resetConversationPage() {
  conversationCursor = conversationNextCursor = "";
  conversationPageVersion++;
  byID("conversation-first-page").disabled = true;
  byID("conversation-next-page").disabled = true;
  byID("conversation-page-status").textContent = "";
}
async function selectConversation(id) {
  selectionVersion++;
  selected = selectedUID = viewedWork = "";
  selectedConversation = id;
  conversationSummary = undefined;
  const pending = pendingMessages.get(messageIdentity());
  byID("message").value = pending?.subject === callerSubject ? pending.text : "";
  byID("message-status").textContent = pending && pending.subject !== callerSubject ? "An unresolved message belongs to another caller. Reconnect as the original caller to retry." : "";
  byID("restart-text").value = pendingConversationActions.get(`${conversationPath()}/restart`)?.input.text || "";
  byID("selected-name").textContent = id;
  clear(byID("conversation"));
  clear(byID("actions"));
  byID("close-conversation").hidden = true;
  byID("restart-form").hidden = true;
  updateMessageControls();
  await readConversation();
}
function updateConversationActionControls() {
  const close = pendingConversationActions.get(`${conversationPath()}/close`);
  const restart = pendingConversationActions.get(`${conversationPath()}/restart`);
  byID("close-conversation").hidden = !selectedConversation || (!close && (!conversationSummary?.active || conversationSummary?.state !== "open"));
  byID("close-conversation").disabled = Boolean(close?.sending) || Boolean(close && close.subject !== callerSubject);
  byID("close-conversation").textContent = close ? "Retry closing conversation" : "Close conversation";
  byID("restart-form").hidden = !selectedConversation || (!restart && (!conversationSummary?.active || conversationSummary?.state !== "closed"));
  byID("restart-conversation").disabled = Boolean(restart?.sending) || Boolean(restart && restart.subject !== callerSubject);
  byID("restart-conversation").textContent = restart ? "Retry fresh attempt" : "Start fresh attempt";
  byID("restart-text").disabled = Boolean(restart);
}
async function readConversation() {
  if (!selectedConversation || !token) return;
  const id = selectedConversation;
  const contextVersion = messageContextVersion;
  const detail = await request(conversationPath(id));
  if (contextVersion !== messageContextVersion || id !== selectedConversation) return;
  conversationSummary = detail.summary;
  const summary = detail.summary;
  const activeAction = Object.values(detail.actions || {}).some(action => action.active && !action.decision);
  const historyStatus = detail.history?.status;
  const rootTurn = (detail.history?.turns || []).filter(turn => !turn.subagent_id).at(-1);
  let progress = summary.phase === "Ready" ? "Request saved — awaiting agent" : "Request saved — preparing Session";
  if (rootTurn?.status === "completed") progress = "Latest saved turn completed";
  if (rootTurn?.status === "failed" || historyStatus === "failed") progress = "Latest agent turn failed";
  if (rootTurn?.status === "cancelled" || historyStatus === "cancelled") progress = "Latest agent turn cancelled";
  if (historyStatus === "in_progress" || rootTurn?.status === "in_progress") progress = "Working";
  if (activeAction) progress = "Needs your approval or decision";
  if (summary.state !== "open") progress = summary.state === "closed" ? (detail.archived ? "Closed — earlier content archived" : "Closed — saved history") : "Closing — cleanup in progress";
  byID("selected-name").textContent = summary.title || summary.conversation_id;
  byID("session-state").textContent = `${progress} · Profile: ${summary.route_id} · Attempt ${summary.attempt}${detail.recovery_required ? " — recovery required" : ""}${summary.observation_unavailable ? " — current Session observation unavailable" : ""}${summary.last_error ? ` — ${summary.last_error}` : ""}`;
  const conversation = byID("conversation");
  clear(conversation);
  if (detail.inputs_archived) renderItem(conversation, "Earlier inputs have been archived and are unavailable here.", "item pending");
  if (detail.history_archived) renderItem(conversation, "Saved turn history has been archived and is unavailable here.", "item pending");
  if (detail.archived && !detail.inputs_archived && !detail.history_archived) renderItem(conversation, "Earlier conversation content has been archived and is unavailable here.", "item pending");
  if (detail.inputs?.length) {
    for (const input of detail.inputs) if (input.text) renderItem(conversation, `You: ${input.text} (saved)`);
  } else if (!detail.inputs_archived) {
    for (const submission of detail.submissions || []) if (submission.text) renderItem(conversation, `You: ${submission.text}${submission.accepted ? "" : " (saved, awaiting delivery)"}`);
  }
  if (!detail.history_archived) for (const item of detail.history?.items || []) renderItem(conversation, item);
  renderActions(detail.actions, summary.active && summary.state === "open" ? summary.session_url : undefined);
  updateMessageControls();
  updateConversationActionControls();
}
function rejectsInput(cause) { return [400, 401, 403, 404, 405, 410, 413, 415, 422].includes(cause.status); }
async function mutateConversation(action, input) {
  if (!token || !selectedConversation) return;
  const path = `${conversationPath()}/${action}`;
  let pending = pendingConversationActions.get(path);
  if (pending?.sending) return;
  if (pending && pending.subject !== callerSubject) {
    error("Reconnect as the original caller to retry this pending request.");
    return;
  }
  if (pending && JSON.stringify(pending.input) !== JSON.stringify(input)) {
    if (action === "restart") byID("restart-text").value = pending.input.text;
    error("Retry the original pending request before changing its text.");
    return;
  }
  if (!pending) {
    pending = { input, key: crypto.randomUUID(), sending: false, subject: callerSubject };
    pendingConversationActions.set(path, pending);
  }
  const contextVersion = messageContextVersion;
  const id = selectedConversation;
  pending.sending = true;
  updateConversationActionControls();
  error("");
  let committed = false;
  try {
    const receipt = await request(path, { method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": pending.key }, body: JSON.stringify(pending.input) });
    confirmReceipt(receipt);
    committed = true;
    pendingConversationActions.delete(path);
    if (contextVersion !== messageContextVersion || selectedConversation !== id) return;
    byID("message-status").textContent = action === "close" ? "Close request saved. Cleanup is in progress." : "First message saved for a fresh attempt. Awaiting turn completion.";
    await selectConversation(receipt.conversation_id);
    await refreshConversations();
  } catch (cause) {
    if (committed) { error(`Request saved; refreshing its history failed: ${cause.message}`); return; }
    if (rejectsInput(cause)) pendingConversationActions.delete(path);
    if (contextVersion === messageContextVersion && selectedConversation === id) error(`${cause.message}${rejectsInput(cause) ? "" : " Acceptance is uncertain; retry the original request."}`);
  } finally {
    pending.sending = false;
    if (contextVersion === messageContextVersion && selectedConversation === id) updateConversationActionControls();
  }
}
function confirmReceipt(receipt) {
  if (receipt?.acceptance !== "committed" || typeof receipt.conversation_id !== "string" || !receipt.conversation_id || !Number.isInteger(receipt.attempt)) throw new Error("Acceptance could not be confirmed. Retry the original request.");
}
async function refreshWork() {
  if (!token) return;
  const contextVersion = messageContextVersion;
  const result = await request(apiPath("/work"));
  if (contextVersion !== messageContextVersion) return;
  byID("tasks-section").hidden = !workConfigured && (result.work || []).length === 0;
  const list = byID("work");
  clear(list);
  for (const work of result.work || []) {
    const li = document.createElement("li");
    const details = document.createElement("div");
    details.textContent = `${work.work_id} — ${work.route_id} — ${work.state} — attempt ${work.attempt}${work.phase ? ` — ${work.phase}` : ""}`;
    li.append(details);
    if (work.last_error) renderItem(li, work.last_error, "item work-error");
    if (work.session_url) {
      const open = document.createElement("button");
      open.type = "button";
      open.textContent = `Open Session ${work.session_name}`;
      open.addEventListener("click", () => selectSession(work.session_name, work.session_uid).catch((cause) => error(cause.message)));
      li.append(open);
    }
    const history = document.createElement("button");
    history.type = "button";
    history.className = "secondary";
    history.textContent = "View work history";
    history.addEventListener("click", () => selectWorkHistory(work).catch((cause) => error(cause.message)));
    li.append(history);
    if (work.state === "open") {
      const close = document.createElement("button");
      close.type = "button";
      close.className = "secondary";
      close.textContent = "Close work";
      close.addEventListener("click", () => closeWork(work).catch((cause) => error(cause.message)));
      li.append(close);
    }
    if (work.id === selectedWork) {
      byID("task-status").textContent = `Work ${work.work_id}: ${work.state}, attempt ${work.attempt}${work.session_uid ? `, Session ${work.session_name}` : ", awaiting Session provisioning"}${work.last_error ? ` — ${work.last_error}` : ""}`;
    }
    list.append(li);
  }
}
async function submitWork(input) {
  return request(apiPath("/work-events"), {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(input),
  });
}
async function closeWork(work) {
  error("");
  let input = pendingClosures.get(work.id);
  if (!input) {
    input = { route_id: work.route_id, work_id: work.work_id, event_id: crypto.randomUUID(), kind: "close" };
    pendingClosures.set(work.id, input);
  }
  const receipt = await submitWork(input);
  pendingClosures.delete(work.id);
  selectedWork = receipt.id;
  await refreshWork();
}
async function selectWorkHistory(work) {
  selectionVersion++;
  selected = "";
  selectedUID = "";
  selectedConversation = "";
  conversationSummary = undefined;
  byID("close-conversation").hidden = true;
  byID("restart-form").hidden = true;
  byID("message").value = "";
  selectedWork = viewedWork = work.id;
  byID("message-status").textContent = "";
  byID("selected-name").textContent = `Work ${work.work_id}`;
  updateMessageControls();
  await readWorkHistory();
}
async function readWorkHistory() {
  if (!viewedWork || !token) return;
  const contextVersion = messageContextVersion;
  const id = viewedWork;
  const detail = await request(apiPath(`/work/${encodeURIComponent(viewedWork)}`));
  if (contextVersion !== messageContextVersion || id !== viewedWork) return;
  const summary = detail.summary;
  byID("session-state").textContent = `Saved work history — ${summary.state}, attempt ${summary.attempt}${summary.last_error ? ` — ${summary.last_error}` : ""}`;
  const conversation = byID("conversation");
  clear(conversation);
  for (const event of detail.work?.events || []) {
    renderItem(conversation, `${event.kind} (attempt ${event.attempt})${event.text ? `: ${event.text}` : ""}`);
  }
  for (const item of detail.conversation?.history?.items || []) renderItem(conversation, item);
  for (const [name, text] of Object.entries(detail.conversation?.notes || {})) {
    renderItem(conversation, `Note ${name}: ${text}`);
  }
  clear(byID("actions"));
}
function renderItem(container, value, className = "item") {
  const div = document.createElement("div");
  div.className = className;
  div.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  container.append(div);
}
async function selectSession(name, uid = "") {
  selectionVersion++;
  const changed = selected !== name || selectedUID !== uid;
  selected = name;
  selectedUID = uid;
  selectedConversation = "";
  conversationSummary = undefined;
  byID("close-conversation").hidden = true;
  byID("restart-form").hidden = true;
  clear(byID("actions"));
  viewedWork = "";
  byID("selected-name").textContent = name;
  byID("message-status").textContent = "";
  if (changed) byID("message").value = pendingMessages.get(messageIdentity())?.text || "";
  updateMessageControls();
  await readSession();
}
async function readSession() {
  if (!selected || !token) return;
  const contextVersion = messageContextVersion;
  const sessionName = selected;
  const sessionUID = selectedUID;
  const sessionNamespace = namespace;
  const base = apiPath(`/sessions/${encodeURIComponent(selected)}`);
  const [record, history, actions] = await Promise.all([
    request(base), request(`${base}/history`), request(`${base}/actions`),
  ]);
  if (contextVersion !== messageContextVersion || sessionName !== selected || sessionUID !== selectedUID || sessionNamespace !== namespace) return;
  byID("session-state").textContent = `${history.history?.status || "Pending"}${history.recovery_required ? " — recovery required" : ""}${history.last_error ? ` — ${history.last_error}` : ""}`;
  const conversation = byID("conversation");
  clear(conversation);
  for (const submission of record.submissions || []) {
    if (submission.text) renderItem(conversation, `You: ${submission.text}${submission.accepted ? "" : " (queued)"}`);
  }
  for (const item of history.history?.items || []) renderItem(conversation, item);
  renderActions(actions, base);
}
function renderActions(actions, base) {
  const actionPanel = byID("actions");
  clear(actionPanel);
  for (const [id, action] of Object.entries(actions || {})) {
    if (!action.active || action.decision) continue;
    renderItem(actionPanel, action.pending, "item pending");
    if (!base) continue;
    for (const decision of ["approve", "deny"]) {
      const button = document.createElement("button");
      button.className = "secondary";
      button.textContent = decision;
      button.addEventListener("click", async () => {
        try {
          await request(`${base}/actions/${encodeURIComponent(id)}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ decision }) });
          await Promise.all([readSession(), readConversation()]);
        } catch (cause) { error(cause.message); }
      });
      actionPanel.append(button);
    }
  }
}
byID("connect-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  error("");
  namespace = byID("namespace").value.trim();
  token = byID("token").value;
  callerSubject = "";
  byID("token").value = "";
  selected = "";
  selectedUID = "";
  selectedConversation = "";
  conversationSummary = undefined;
  for (const id of ["profile", "first-message", "conversation-label"]) byID(id).value = "";
  resetConversationPage();
  byID("conversation-status").textContent = "";
  resetMessageContext();
  byID("workspace").hidden = true;
  const contextVersion = messageContextVersion;
  try {
    await refresh();
    if (contextVersion !== messageContextVersion) return;
    byID("workspace").hidden = false;
    byID("disconnect").disabled = false;
    byID("connection-status").textContent = `Connected to ${namespace}`;
    clearInterval(pollTimer);
    pollTimer = setInterval(() => Promise.all([readSession(), refreshWork(), readWorkHistory(), readConversation(), refreshConversations()]).catch((cause) => error(cause.message)), 2000);
  } catch (cause) {
    if (contextVersion !== messageContextVersion) return;
    token = "";
    error(cause.message);
  }
});
byID("disconnect").addEventListener("click", () => {
  clearInterval(pollTimer);
  token = namespace = callerSubject = selected = selectedUID = selectedWork = viewedWork = selectedConversation = "";
  conversationSummary = undefined;
  resetConversationPage();
  resetMessageContext();
  workConfigured = false;
  pendingTask = undefined;
  pendingClosures.clear();
  byID("workspace").hidden = true;
  byID("disconnect").disabled = true;
  byID("connection-status").textContent = "Disconnected";
  clear(byID("conversation"));
  clear(byID("actions"));
  clear(byID("work"));
  clear(byID("conversations"));
  byID("conversation-status").textContent = "";
  for (const id of ["profile", "first-message", "conversation-label"]) byID(id).value = "";
  byID("task-status").textContent = "";
  error("");
});
byID("profile").addEventListener("change", updateConversationControls);
byID("conversation-first-page").addEventListener("click", () => changeConversationPage("").catch(cause => error(cause.message)));
byID("conversation-next-page").addEventListener("click", () => {
  if (!conversationNextCursor) return;
  return changeConversationPage(conversationNextCursor).catch(cause => error(cause.message));
});
byID("conversation-form").addEventListener("submit", async event => {
  event.preventDefault();
  if (!token) return;
  const requestNamespace = namespace;
  let pending = pendingConversations.get(requestNamespace);
  if (pending?.sending) return;
  if (pending && pending.subject !== callerSubject) {
    error("Reconnect as the original caller to retry the pending conversation request.");
    return;
  }
  const input = { route_id: byID("profile").value, text: byID("first-message").value };
  const title = byID("conversation-label").value.trim();
  if (title) input.title = title;
  if (pending && JSON.stringify(pending.input) !== JSON.stringify(input)) {
    updateConversationControls();
    error("Retry the original pending request before changing its profile or message.");
    return;
  }
  if (!pending) {
    const profile = profiles.find(route => route.id === input.route_id);
    if (!profile?.ready) { error("Select a ready approved profile."); return; }
    pending = { input, key: crypto.randomUUID(), sending: false, subject: callerSubject };
    pendingConversations.set(requestNamespace, pending);
  }
  const contextVersion = messageContextVersion;
  const previousSelection = selectionVersion;
  pending.sending = true;
  updateConversationControls();
  error("");
  let committed = false;
  try {
    const receipt = await request(`/v1/namespaces/${encodeURIComponent(requestNamespace)}/conversations`, { method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": pending.key }, body: JSON.stringify(pending.input) });
    confirmReceipt(receipt);
    committed = true;
    pendingConversations.delete(requestNamespace);
    if (namespace !== requestNamespace || callerSubject !== pending.subject || !token) return;
    byID("first-message").value = "";
    byID("conversation-label").value = "";
    byID("conversation-status").textContent = `First message saved. Conversation ${receipt.conversation_id}, attempt ${receipt.attempt}. Awaiting turn completion.`;
    if (contextVersion === messageContextVersion && previousSelection === selectionVersion) await selectConversation(receipt.conversation_id);
    await refreshConversations();
  } catch (cause) {
    if (committed) { error(`First message saved; refreshing its history failed: ${cause.message}`); return; }
    if (rejectsInput(cause)) pendingConversations.delete(requestNamespace);
    if (namespace === requestNamespace && callerSubject === pending.subject && token) {
      error(cause.message);
      byID("conversation-status").textContent = rejectsInput(cause) ? "Request rejected. Correct the input before submitting." : "Acceptance is uncertain. Retry the original request with the same profile and message. Keep this page open; reload loses its retry identity. Saved conversations can show requests that were accepted.";
    }
  } finally {
    pending.sending = false;
    if (namespace === requestNamespace && token) updateConversationControls();
  }
});
byID("close-conversation").addEventListener("click", () => mutateConversation("close", {}));
byID("restart-form").addEventListener("submit", event => {
  event.preventDefault();
  return mutateConversation("restart", { text: byID("restart-text").value });
});
byID("refresh").addEventListener("click", () => refresh().catch((cause) => error(cause.message)));
byID("spend-mode").addEventListener("change", () => {
  const limited = byID("spend-mode").value === "limited";
  byID("spend-limit-label").hidden = !limited;
  byID("spend-limit").required = limited;
});
byID("create-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  error("");
  try {
    const extra = byID("extra-spec").value.trim();
    const spec = extra ? JSON.parse(extra) : {};
    if (!spec || typeof spec !== "object" || Array.isArray(spec)) throw new Error("Additional spec must be a JSON object");
    spec.agentRef = { name: byID("agent").value };
    spec.environmentRef = { name: byID("environment").value };
    const mode = byID("spend-mode").value;
    spec.spendControl = mode === "limited" ? { mode, limitCents: Number(byID("spend-limit").value) } : { mode };
    const created = await request(apiPath("/sessions"), {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: byID("session-name").value.trim(), spec }),
    });
    await refresh();
    await selectSession(created.name, created.uid);
  } catch (cause) { error(cause.message); }
});
byID("task-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  error("");
  try {
    const routeID = byID("work-route").value;
    const workID = byID("work-id").value.trim();
    const kind = byID("work-event-kind").value;
    const text = byID("task-text").value;
    if (!pendingTask || pendingTask.route_id !== routeID || pendingTask.work_id !== workID || pendingTask.kind !== kind || pendingTask.text !== text) {
      pendingTask = { route_id: routeID, work_id: workID, event_id: crypto.randomUUID(), kind, text };
    }
    const receipt = await submitWork(pendingTask);
    selectedWork = receipt.id;
    pendingTask = undefined;
    byID("task-text").value = "";
    byID("task-status").textContent = `Accepted work ${receipt.work_id}, attempt ${receipt.attempt}. Preparing its Session.`;
    await refreshWork();
  } catch (cause) { error(cause.message); }
});
byID("message-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if ((!selected && !selectedConversation) || !token) return;
  if (selectedConversation && !pendingMessages.has(messageIdentity()) && (!conversationSummary?.active || conversationSummary.state !== "open")) return;
  const path = messagePath();
  const identity = messageIdentity();
  let pending = pendingMessages.get(identity);
  if (pending?.sending) return;
  if (pending && pending.subject !== callerSubject) {
    error("Reconnect as the original caller to retry this pending message.");
    return;
  }
  error("");
  const text = byID("message").value;
  if (pending && pending.text !== text) {
    byID("message").value = pending.text;
    error("Retry the pending message before sending different text.");
    return;
  }
  if (!pending) {
    pending = { namespace, session: selected, uid: selectedUID, conversation: selectedConversation, subject: callerSubject, text, key: crypto.randomUUID(), sending: false };
    pendingMessages.set(identity, pending);
  }
  const contextVersion = messageContextVersion;
  pending.sending = true;
  updateMessageControls();
  try {
    const receipt = await request(`${path}/messages`, {
      method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": pending.key },
      body: JSON.stringify({ text: pending.text }),
    });
    if (pending.conversation) confirmReceipt(receipt);
    pendingMessages.delete(identity);
    if (contextVersion !== messageContextVersion) return;
    if (namespace === pending.namespace && selected === pending.session && selectedUID === pending.uid && selectedConversation === pending.conversation) {
      if (byID("message").value === pending.text) byID("message").value = "";
      byID("message-status").textContent = "Message saved. Awaiting turn completion.";
      await Promise.all([readSession(), readConversation()]);
    }
  } catch (cause) {
    if (contextVersion !== messageContextVersion) return;
    // These statuses reject input before acceptance; network, decoding, and
    // persistence failures leave acceptance uncertain and retain the same key.
    if (pendingMessages.get(identity) === pending && rejectsInput(cause)) {
      pendingMessages.delete(identity);
    }
    if (namespace === pending.namespace && selected === pending.session && selectedUID === pending.uid && selectedConversation === pending.conversation) error(cause.message);
  } finally {
    pending.sending = false;
    if (contextVersion === messageContextVersion || (pending.conversation && namespace === pending.namespace && selectedConversation === pending.conversation)) updateMessageControls();
  }
});
