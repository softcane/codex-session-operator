package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	_testDeletePolicy       = "Delete"
	_catalogEnvironmentName = "catalog-environment"
	_catalogVaultName       = "catalog-vault"
	_catalogRouteID         = "approved"
)

func TestSessionManagementRequiresExplicitChoicesAndNamespace(t *testing.T) {
	store := testStore(t)
	g, writer := testGateway(t, store, simulatorForTest())
	if err := g.EnableSessionManagement(writer); err != nil {
		t.Fatal(err)
	}
	endpoint := "/v1/namespaces/team/sessions"
	for _, body := range []string{
		`{"name":"investigation","spec":{"agentRef":{"name":"agent"},"environmentRef":{"name":"env"}}}`,
		`{"name":"investigation","spec":{"environmentRef":{"name":"env"},"spendControl":{"mode":"unlimited"}}}`,
		`{"name":"investigation","spec":{"agentRef":{"name":"agent"},"spendControl":{"mode":"unlimited"}}}`,
		`{"name":"investigation","spec":{"agentRef":{"name":"agent"},"environmentRef":{"name":"env"},"spendControl":{"mode":"limited","limitCents":0}}}`,
	} {
		if got := request(g, http.MethodPost, endpoint, _token, "", body).Code; got != http.StatusBadRequest {
			t.Fatalf("invalid create returned %d for %s", got, body)
		}
	}
	body := `{"name":"investigation","spec":{"agentRef":{"name":"agent"},"environmentRef":{"name":"env"},"spendControl":{"mode":"limited","limitCents":150}}}`
	if got := request(g, http.MethodPost, endpoint, "invalid", "", body).Code; got != http.StatusUnauthorized {
		t.Fatalf("unauthenticated create: %d", got)
	}
	if got := request(g, http.MethodPost, "/v1/namespaces/other/sessions", _token, "", body).Code; got != http.StatusNotFound {
		t.Fatalf("cross-namespace create: %d", got)
	}
	if got := request(g, http.MethodGet, "/v1/namespaces/other/sessions", _token, "", "").Code; got != http.StatusNotFound {
		t.Fatalf("cross-namespace list: %d", got)
	}
	response := request(g, http.MethodPost, endpoint, _token, "", body)
	if response.Code != http.StatusCreated || response.Header().Get("Location") != endpoint+"/investigation" {
		t.Fatalf("create: %d %s", response.Code, response.Body.String())
	}
	var saved agentsv1alpha1.Session
	if err := writer.Get(t.Context(), client.ObjectKey{Namespace: _testNamespace, Name: "investigation"}, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Spec.AgentRef.Name != _auditAgentName || saved.Spec.EnvironmentRef.Name != "env" || *saved.Spec.SpendControl.LimitCents != 150 {
		t.Fatalf("unexpected saved Session: %+v", saved.Spec)
	}
	if got := request(g, http.MethodPost, endpoint, _token, "", body).Code; got != http.StatusConflict {
		t.Fatalf("duplicate create: %d", got)
	}
	response = request(g, http.MethodGet, endpoint, _token, "", "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"name":"investigation"`) || strings.Contains(response.Body.String(), "openaiSessionID") {
		t.Fatalf("unsafe Session list: %d %s", response.Code, response.Body.String())
	}
}

func TestCatalogExposesOnlyNamesReadinessAndMode(t *testing.T) {
	store := testStore(t)
	agent := &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "private-agent", Namespace: _testNamespace}, Spec: agentsv1alpha1.AgentSpec{Model: "gpt-6-luna", DeletionPolicy: _testDeletePolicy}}
	instructions := "confidential instructions"
	agent.Spec.Instructions = &instructions
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "compute", Namespace: _testNamespace}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: "none", DeletionPolicy: _testDeletePolicy}}
	vault := &agentsv1alpha1.Vault{ObjectMeta: metav1.ObjectMeta{Name: "secrets", Namespace: _testNamespace}, Spec: agentsv1alpha1.VaultSpec{DeletionPolicy: _testDeletePolicy}}
	g, _ := testGateway(t, store, simulatorForTest(), agent, environment, vault)
	response := request(g, http.MethodGet, "/v1/namespaces/team/catalog", _token, "", "")
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), instructions) || strings.Contains(response.Body.String(), "spec") {
		t.Fatalf("unsafe catalog: %d %s", response.Code, response.Body.String())
	}
	var catalog map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &catalog); err != nil {
		t.Fatal(err)
	}
	if len(catalog["agents"]) == 0 || len(catalog["environments"]) == 0 || len(catalog["vaults"]) == 0 {
		t.Fatalf("missing catalog items: %s", response.Body.String())
	}
}

func TestWorkRouteCatalogRequiresCurrentReadyResourcesInPrincipalNamespace(t *testing.T) {
	tests := []struct {
		name      string
		stale     string
		deleting  bool
		wantReady bool
	}{
		{name: "current", wantReady: true},
		{name: "stale agent", stale: _auditAgentName},
		{name: "stale environment", stale: _catalogEnvironmentName},
		{name: "stale vault", stale: _catalogVaultName},
		{name: "deleting agent", deleting: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ready := func(resource string) []metav1.Condition {
				observed := int64(2)
				if tt.stale == resource {
					observed = 1
				}
				return []metav1.Condition{{Type: _readyCondition, Status: metav1.ConditionTrue, ObservedGeneration: observed}}
			}
			instructions := "private prompt must remain private"
			agent := &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: _auditAgentName, Namespace: _testNamespace, Generation: 2},
				Spec: agentsv1alpha1.AgentSpec{Instructions: &instructions}, Status: agentsv1alpha1.AgentStatus{Conditions: ready(_auditAgentName)}}
			if tt.deleting {
				now := metav1.Now()
				agent.DeletionTimestamp = &now
				agent.Finalizers = []string{"test.cleanup"}
			}
			environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: _catalogEnvironmentName, Namespace: _testNamespace, Generation: 2},
				Status: agentsv1alpha1.EnvironmentStatus{Conditions: ready(_catalogEnvironmentName)}}
			vault := &agentsv1alpha1.Vault{ObjectMeta: metav1.ObjectMeta{Name: _catalogVaultName, Namespace: _testNamespace, Generation: 2},
				Status: agentsv1alpha1.VaultStatus{Conditions: ready(_catalogVaultName)}}
			g, _ := testGateway(t, testStore(t), simulatorForTest(), agent, environment, vault)
			limit := int64(150)
			g.workRoutes = map[string]WorkRoute{
				_catalogRouteID: {ID: _catalogRouteID, Namespace: _testNamespace, Source: "manual", Agent: _auditAgentName, Environment: _catalogEnvironmentName, Vaults: []string{_catalogVaultName}, SpendControl: &agentsv1alpha1.SessionSpendControl{Mode: _scenarioLimited, LimitCents: &limit}},
				"missing":       {ID: "missing", Namespace: _testNamespace, Source: "manual", Agent: _auditAgentName, Environment: "absent"},
				"outside":       {ID: "outside", Namespace: "another-team", Source: "private-source", Agent: "hidden-agent", Environment: "hidden-env"},
			}
			response := request(g, http.MethodGet, "/v1/namespaces/team/catalog", _token, "", "")
			if response.Code != http.StatusOK {
				t.Fatalf("catalog: %d %s", response.Code, response.Body.String())
			}
			var catalog struct {
				Subject string             `json:"caller_subject"`
				Routes  []workRouteSummary `json:"work_routes"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &catalog); err != nil {
				t.Fatal(err)
			}
			if len(catalog.Routes) != 2 || catalog.Routes[0].ID != _catalogRouteID || catalog.Routes[0].Ready != tt.wantReady || catalog.Routes[1].Ready {
				t.Fatalf("unexpected routes: %+v", catalog.Routes)
			}
			policy := catalog.Routes[0].SpendControl
			if catalog.Subject != _testConversationSubject || policy == nil || policy.Mode != _scenarioLimited || policy.LimitCents == nil || *policy.LimitCents != limit || len(catalog.Routes[0].Vaults) != 1 || catalog.Routes[0].Vaults[0] != _catalogVaultName {
				t.Fatalf("catalog lacks caller identity or approved policy metadata: %+v", catalog)
			}
			for _, private := range []string{instructions, "private-source", "hidden-agent", "openaiSessionID"} {
				if strings.Contains(response.Body.String(), private) {
					t.Fatalf("catalog exposed %q", private)
				}
			}
		})
	}
}

func TestTaskInterfaceSubmitsAndReusesWorkWithStableRetryIdentity(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is required for the interface event-handler test")
	}
	script, err := uiFiles.ReadFile("ui/app.js")
	if err != nil {
		t.Fatal(err)
	}
	page, err := uiFiles.ReadFile("ui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"tasks-section", "task-form", "work-route", "work-id", "work-event-kind", "task-text", "task-status", "work"} {
		if !strings.Contains(string(page), `id="`+id+`"`) {
			t.Fatalf("task interface is missing %s", id)
		}
	}
	const harness = `
const assert = require("node:assert/strict");
const vm = require("node:vm");
const source = require("node:fs").readFileSync(0, "utf8");
class Element {
  constructor() { this.value = ""; this.textContent = ""; this.children = []; this.listeners = {}; }
  replaceChildren() { this.children = []; }
  append(...children) { this.children.push(...children); }
  addEventListener(type, handler) { this.listeners[type] = handler; }
}
const elements = new Map();
globalThis.document = {
  getElementById(id) { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); },
  createElement() { return new Element(); },
  createTextNode(text) { return {textContent:text}; },
};
globalThis.crypto = require("node:crypto");
globalThis.setInterval = () => 1;
globalThis.clearInterval = () => {};
const calls = [];
const work = [];
let failFirstTask = true;
let showRoutes = true;
globalThis.fetch = async (path, options) => {
  calls.push({path,options});
  assert.equal(options.headers.Authorization, "Bearer test-private-token");
  let data;
  if (path.endsWith("/catalog")) data = {agents:[],environments:[],work_routes:showRoutes ? [{id:"payments",source:"manual",agent:"agent",environment:"env",ready:true}] : []};
  else if (path.endsWith("/sessions")) { assert.notEqual(options.method, "POST"); data = {sessions:[]}; }
  else if (path.endsWith("/work")) data = {work};
  else if (path.endsWith("/conversations")) data = {conversations:[]};
  else if (path.endsWith("/work-events")) {
    const input = JSON.parse(options.body);
    assert.deepEqual(Object.keys(input).sort(), input.kind === "close" ? ["event_id","kind","route_id","work_id"] : ["event_id","kind","route_id","text","work_id"]);
    assert.equal(input.route_id, "payments");
    assert.equal(input.work_id, "incident-123");
    if (input.kind === "task" && failFirstTask) { failFirstTask = false; throw new Error("lost response"); }
    if (!work.length) work.push({id:"opaque-work",route_id:input.route_id,work_id:input.work_id,source:"manual",state:"open",attempt:1,session_name:"session-1",session_uid:"uid-1",session_url:"/v1/namespaces/team/sessions/session-1",phase:"Ready"});
    if (input.kind === "close") work[0].state = "closing";
    if (input.kind === "restart") {
      assert.equal(work[0].state,"closed");
      work[0].state = "open";
      work[0].attempt = 2;
      work[0].session_name = "session-2";
      work[0].session_url = "/v1/namespaces/team/sessions/session-2";
    }
    data = {id:work[0].id,work_id:input.work_id,attempt:work[0].attempt,work_url:"/v1/namespaces/team/work/opaque-work"};
  } else if (path.endsWith("/work/opaque-work")) {
    data = {summary:work[0],work:{events:[{kind:"task",attempt:1,text:"Saved <img src=x onerror=alert(1)> evidence"}]},conversation:{history:{items:["Investigation findings"]},notes:{finding:"Decimal truncated"}}};
  } else throw new Error("unexpected path " + path);
  return {ok:true,json:async () => data};
};
vm.runInThisContext(source);
(async () => {
  const el = document.getElementById;
  const fire = (id,type) => el(id).listeners[type]({preventDefault(){}});
  el("namespace").value = "team";
  el("token").value = "test-private-token";
  await fire("connect-form","submit");
  assert.equal(el("token").value, "");
  assert.equal(el("tasks-section").hidden, false);
  el("work-route").value = "payments";
  el("work-id").value = "incident-123";
  el("work-event-kind").value = "task";
  el("task-text").value = "Investigate <img src=x onerror=alert(1)>";
  await fire("task-form","submit");
  assert.match(el("error").textContent,/lost response/);
  await fire("task-form","submit");
  let events = calls.filter(c => c.path.endsWith("/work-events")).map(c => JSON.parse(c.options.body));
  assert.equal(events[0].event_id, events[1].event_id);
  assert.equal(el("task-text").value, "");
  assert.match(el("task-status").textContent,/Session session-1/);
  el("task-text").value = "More evidence";
  await fire("task-form","submit");
  assert.equal(work.length, 1);
  assert.equal(work[0].attempt, 1);
  events = calls.filter(c => c.path.endsWith("/work-events")).map(c => JSON.parse(c.options.body));
  assert.notEqual(events[1].event_id, events[2].event_id);
  el("work-event-kind").value = "evidence";
  el("task-text").value = "New logs";
  await fire("task-form","submit");
  events = calls.filter(c => c.path.endsWith("/work-events")).map(c => JSON.parse(c.options.body));
  assert.equal(events.at(-1).kind,"evidence");
  assert.equal(work[0].attempt,1);
  const close = el("work").children[0].children.find(c => c.textContent === "Close work");
  await close.listeners.click();
  assert.equal(work[0].state, "closing");
  work[0].state = "closed";
  work[0].session_url = "";
  await el("refresh").listeners.click();
  assert.equal(el("work").children[0].children.some(c => c.textContent.startsWith("Open Session")),false);
  const history = el("work").children[0].children.find(c => c.textContent === "View work history");
  await history.listeners.click();
  assert.equal(el("send").disabled,true);
  assert.match(el("session-state").textContent,/Saved work history — closed/);
  assert.match(el("conversation").children[0].textContent,/Saved <img/);
  assert.equal(el("conversation").children[1].textContent,"Investigation findings");
  showRoutes = false;
  await el("refresh").listeners.click();
  assert.equal(el("tasks-section").hidden,false);
  assert.equal(el("task-form").hidden,true);
  showRoutes = true;
  await el("refresh").listeners.click();
  el("work-event-kind").value = "restart";
  el("task-text").value = "Fresh investigation";
  await fire("task-form","submit");
  assert.equal(work.length,1);
  assert.equal(work[0].attempt,2);
  assert.match(el("task-status").textContent,/Session session-2/);
  await fire("disconnect","click");
  assert.equal(el("workspace").hidden, true);
  assert.equal(el("work").children.length, 0);
})().catch(error => {console.error(error);process.exitCode=1;});
`
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, "-e", harness)
	command.Stdin = strings.NewReader(string(script))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("task interface: %v\n%s", err, output)
	}
}

func TestInterfaceAndReadinessDoNotExposeCredentials(t *testing.T) {
	store := testStore(t)
	g, _ := testGateway(t, store, simulatorForTest())
	for _, path := range []string{"/ui", "/ui/app.js", "/ui/style.css"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		g.ServeHTTP(response, req)
		if response.Code != http.StatusOK || response.Header().Get("Content-Security-Policy") == "" || strings.Contains(response.Body.String(), _token) {
			t.Fatalf("unsafe interface %s: %d", path, response.Code)
		}
	}
	if got := request(g, http.MethodGet, "/readyz", "", "", "").Code; got != http.StatusOK {
		t.Fatalf("worker readiness: %d", got)
	}
	if got := request(g, http.MethodGet, "/v1/namespaces/team/sessions", "invalid", "", "").Code; got != http.StatusUnauthorized {
		t.Fatalf("Session list requires auth: %d", got)
	}
}

func TestConversationInterfaceRetryAndLifecycle(t *testing.T) {
	t.Parallel()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is required for the interface event-handler test")
	}
	script, err := uiFiles.ReadFile("ui/app.js")
	if err != nil {
		t.Fatal(err)
	}
	page, err := uiFiles.ReadFile("ui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"conversation-form", "profile", "profile-details", "first-message", "conversation-label", "conversations", "close-conversation", "restart-form"} {
		if !strings.Contains(string(page), `id="`+id+`"`) {
			t.Fatalf("conversation interface is missing %s", id)
		}
	}
	const harness = `
const assert = require("node:assert/strict");
const vm = require("node:vm");
const source = require("node:fs").readFileSync(0, "utf8");
const scenario = process.argv[1];
class Element {
  constructor() { this.value = ""; this.textContent = ""; this.children = []; this.listeners = {}; this.disabled = false; }
  replaceChildren() { this.children = []; }
  append(...children) { this.children.push(...children); }
  addEventListener(type, handler) { this.listeners[type] = handler; }
}
const elements = new Map();
globalThis.document = {
  getElementById(id) { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); },
  createElement() { return new Element(); },
  createTextNode(text) { return {textContent:text}; },
};
globalThis.crypto = require("node:crypto");
globalThis.setInterval = () => 1;
globalThis.clearInterval = () => {};
const calls = [];
const receipts = new Map();
const summaries = [{conversation_id:"existing", title:"Existing task", route_id:"payments", attempt:1, active:true, state:"open"}];
let first = true;
let release;
let detailRelease;
let detailBlocked = false;
let actionFailed = false;
let refreshFailed = false;
let messageFailed = false;
const summary = id => summaries.find(s => s.conversation_id === id);
globalThis.fetch = async (path, options) => {
  assert.equal(options.headers.Authorization.startsWith("Bearer "), true);
  if (options.method === "POST" && path.includes("/conversations")) {
    const input = JSON.parse(options.body);
    const key = options.headers["Idempotency-Key"];
    calls.push({path, key, body:options.body, auth:options.headers.Authorization});
    const identity = path + options.headers.Authorization + key;
    let receipt = receipts.get(identity);
    if (receipt) assert.equal(receipt.body, options.body);
    else {
      const id = path.endsWith("/conversations") ? "created" : path.split("/").at(-2);
      if (path.endsWith("/conversations")) summaries.push({conversation_id:id, title:input.title, route_id:input.route_id, attempt:1, active:true, state:"open"});
      if (path.endsWith("/close")) summary(id).state = "closed";
      let resultID = id;
      if (path.endsWith("/restart")) {
        assert.equal(summary(id).state, "closed");
        summary(id).active = false;
        resultID = "restarted";
        summaries.push({...summary(id), conversation_id:resultID, attempt:2, active:true, state:"open"});
      }
      receipt = {body:options.body, value:{conversation_id:resultID, conversation_url:"/conversations/"+resultID, attempt:summary(resultID).attempt, acceptance:"committed"}};
      receipts.set(identity, receipt);
    }
    if (path.endsWith("/conversations") && first) {
      first = false;
      if (["lost_response", "changed_profile", "changed_text", "reconnect", "different_subject", "namespace_switch"].includes(scenario)) throw new Error("lost response");
      if (scenario === "server_failure") return {ok:false,status:503,text:async()=>"store unavailable"};
      if (scenario === "decode_failure") return {ok:true,json:async()=>{throw new Error("truncated response");}};
      if (["concurrent_submission", "selection_race", "reconnect_inflight"].includes(scenario)) await new Promise(resolve => {release=resolve;});
    }
    if (!actionFailed && ((scenario === "close_uncertain" && path.endsWith("/close")) || (scenario === "restart_uncertain" && path.endsWith("/restart")))) {actionFailed=true; throw new Error("lost action response");}
    if (!messageFailed && path.endsWith("/messages") && ["message_reconnect", "message_subject"].includes(scenario)) {messageFailed=true;throw new Error("lost message response");}
    return {ok:true,json:async()=>receipt.value};
  }
  let data;
  if (path.endsWith("/catalog")) data = {caller_subject:options.headers.Authorization === "Bearer bob-token" ? "bob" : "alice", agents:[],environments:[],work_routes:[
    {id:"payments",agent:"investigator",environment:"text-only",ready:true,spend_control:{mode:"limited",limitCents:150}},
    {id:"browser",agent:"browser-agent",environment:"desktop",ready:true,spend_control:{mode:"unlimited"}},
  ]};
  else if (path.endsWith("/sessions")) data = {sessions:[]};
  else if (path.endsWith("/work")) data = {work:[]};
  else if (path.endsWith("/conversations")) data = {conversations:summaries};
  else if (path.includes("/conversations/")) {
    const id = path.split("/").at(-1);
    if (scenario === "refresh_failure" && id === "created" && !refreshFailed) {refreshFailed=true;throw new Error("history unavailable");}
    if (detailBlocked && id === "created") {detailBlocked=false;await new Promise(resolve=>{detailRelease=resolve;});}
    data = {summary:summary(id),submissions:[{text:"Saved <img src=x onerror=alert(1)>",accepted:false}],history:{items:["Findings for " + id]},actions:{}};
    if (scenario === "queued_input") {data.inputs=[{text:"Accepted before provisioning"}];data.submissions=[];}
    if (scenario === "status_preparing") {data.summary.session_uid="bound-uid";data.history.status="gateway.none";}
    if (scenario === "status_working") data.history.status="in_progress";
    if (scenario === "status_completed") {data.history.status="idle";data.history.turns=[{status:"completed",subagent_id:null},{status:"in_progress",subagent_id:"child"}];}
    if (scenario === "status_failed") data.history.turns=[{status:"failed"}];
    if (scenario === "status_cancelled") data.history.turns=[{status:"cancelled"}];
    if (scenario === "archived_input") {data.archived=true;data.inputs_archived=true;data.summary.state="closed";}
    if (scenario === "archived_history") {data.archived=true;data.history_archived=true;data.history.items=[];data.summary.state="closed";}
  } else throw new Error("unexpected path " + path);
  return {ok:true,json:async()=>data};
};
vm.runInThisContext(source);
(async () => {
  const el = document.getElementById;
  const fire = (id,type) => el(id).listeners[type]({preventDefault(){}});
  const connect = async (ns="team", auth="alice-token") => {el("namespace").value=ns;el("token").value=auth;await fire("connect-form","submit");};
  const submit = () => fire("conversation-form","submit");
  const open = id => {
    const index = summaries.findIndex(s=>s.conversation_id === id);
    return el("conversations").children[index].children[0].listeners.click();
  };
  await connect();
  assert.equal(el("token").value, "");
  el("profile").value="payments";
  await fire("profile","change");
  assert.match(el("profile-details").textContent,/Agent: investigator/);
  assert.match(el("profile-details").textContent,/Environment: text-only/);
  assert.match(el("profile-details").textContent,/150 US cents/);
  el("first-message").value="Investigate <img src=x onerror=alert(1)>";
  el("conversation-label").value="Checkout failure";
  if (["concurrent_submission", "selection_race", "reconnect_inflight"].includes(scenario)) {
    const pending = submit();
    assert.equal(typeof release,"function");
    assert.equal(el("start-conversation").disabled,true);
    assert.equal(el("profile").disabled,true);
    assert.equal(el("first-message").disabled,true);
    if (scenario === "concurrent_submission") {await submit();assert.equal(calls.length,1);}
    else if (scenario === "reconnect_inflight") {await fire("disconnect","click");await connect();assert.equal(el("start-conversation").disabled,true);}
    else await open("existing");
    release();await pending;
    if (scenario === "selection_race") {assert.equal(el("selected-name").textContent,"Existing task");return;}
    if (scenario === "reconnect_inflight") {assert.equal(el("start-conversation").disabled,false);assert.equal(el("first-message").value, "");assert.match(el("conversation-status").textContent,/First message saved/);assert.equal(calls.length,1);return;}
  } else await submit();
  if (["lost_response", "changed_profile", "changed_text", "reconnect", "different_subject", "namespace_switch", "server_failure", "decode_failure"].includes(scenario)) {
    assert.match(el("conversation-status").textContent,/Acceptance is uncertain/);
    assert.equal(el("profile").disabled,true);
    assert.equal(el("first-message").disabled,true);
    if (scenario === "changed_profile" || scenario === "changed_text") {
      el(scenario === "changed_profile" ? "profile" : "first-message").value = scenario === "changed_profile" ? "browser" : "Changed task";
      await submit();
      assert.equal(calls.length,1);
      assert.match(el("error").textContent,/original pending request/);
      assert.equal(el("profile").value,"payments");
      assert.equal(el("first-message").value,"Investigate <img src=x onerror=alert(1)>");
    }
    if (["reconnect", "different_subject", "namespace_switch"].includes(scenario)) {
      await fire("disconnect","click");
      assert.equal(el("first-message").value, "");
      await connect(scenario === "namespace_switch" ? "other-team" : "team", scenario === "different_subject" ? "bob-token" : "alice-token");
      if (scenario === "different_subject") {
        assert.equal(el("start-conversation").disabled,true);
        await submit();assert.equal(calls.length,1);
        assert.match(el("error").textContent,/original caller/);
        await connect();
      }
      if (scenario === "namespace_switch") {
        assert.equal(el("first-message").value, "");
        el("profile").value="browser";await fire("profile","change");
        el("first-message").value="New namespace task";
        await submit();
        assert.match(calls[1].path,/other-team\/conversations$/);
        assert.notEqual(calls[0].key,calls[1].key);
        return;
      }
    }
    await submit();
    assert.equal(calls[0].key,calls.at(-1).key);
    assert.equal(calls[0].body,calls.at(-1).body);
    assert.equal(receipts.size,1);
  }
  assert.equal(el("first-message").value, "");
  assert.equal(el("profile").disabled,false);
  assert.match(el("conversation-status").textContent,/First message saved/);
  assert.doesNotMatch(el("conversation-status").textContent,/finished|completed/);
  if (scenario === "refresh_failure") {assert.match(el("error").textContent,/First message saved/);assert.equal(calls.length,1);return;}
  if (scenario === "queued_input") {assert.equal(el("conversation").children[0].textContent,"You: Accepted before provisioning (saved)");return;}
  if (scenario === "archived_input") {
    assert.match(el("session-state").textContent,/earlier content archived/);
    assert.match(el("conversation").children[0].textContent,/inputs have been archived/);
    assert.equal(el("conversation").children.some(c=>c.textContent.includes("Saved <img")),false);
    return;
  }
  if (scenario === "archived_history") {assert.match(el("conversation").children[0].textContent,/turn history has been archived/);return;}
  assert.equal(el("conversation").children[0].textContent,"You: Saved <img src=x onerror=alert(1)> (saved, awaiting delivery)");
  if (scenario.startsWith("status_")) {
    const expected={status_preparing:/Request saved — preparing Session/,status_working:/Working/,status_completed:/Latest saved turn completed/,status_failed:/Latest agent turn failed/,status_cancelled:/Latest agent turn cancelled/};
    assert.match(el("session-state").textContent,expected[scenario]);
    assert.doesNotMatch(el("session-state").textContent,/gateway\.none/);
    return;
  }
  if (["message_reconnect", "message_subject"].includes(scenario)) {
    el("message").value="Retain this message";await fire("message-form","submit");
    const original=calls.at(-1);assert.match(el("error").textContent,/lost message response/);
    await fire("disconnect","click");await connect("team",scenario === "message_subject" ? "bob-token" : "alice-token");await open("created");
    if (scenario === "message_subject") {
      assert.equal(el("send").disabled,true);assert.equal(el("message").value, "");
      const before=calls.length;await fire("message-form","submit");assert.equal(calls.length,before);
      assert.match(el("error").textContent,/original caller/);
      await connect();await open("created");
    }
    assert.equal(el("message").value,"Retain this message");
    await fire("message-form","submit");assert.equal(calls.at(-1).key,original.key);assert.equal(calls.at(-1).body,original.body);
    return;
  }
  if (scenario === "detail_race") {
    detailBlocked=true;
    const pending=open("created");
    assert.equal(typeof detailRelease,"function");
    await open("existing");detailRelease();await pending;
    assert.equal(el("selected-name").textContent,"Existing task");
    assert.equal(el("conversation").children[1].textContent,"Findings for existing");
    return;
  }
  if (["lifecycle", "close_uncertain", "restart_uncertain"].includes(scenario)) {
    el("message").value="More evidence";await fire("message-form","submit");
    assert.match(calls.at(-1).path,/created\/messages$/);
    assert.match(el("message-status").textContent,/Message saved/);
    await fire("close-conversation","click");
    if (scenario === "close_uncertain") {
      await fire("refresh","click");await open("created");
      assert.equal(el("close-conversation").hidden,false);
      const previous=calls.at(-1);await fire("close-conversation","click");
      assert.equal(previous.key,calls.at(-1).key);
    }
    assert.equal(el("send").disabled,true);
    assert.equal(el("restart-form").hidden,false);
    el("restart-text").value="Fresh investigation";await fire("restart-form","submit");
    if (scenario === "restart_uncertain") {
      await fire("refresh","click");await open("created");
      assert.equal(el("restart-form").hidden,false);
      const previous=calls.at(-1);await fire("restart-form","submit");
      assert.equal(previous.key,calls.at(-1).key);
      assert.equal(previous.body,calls.at(-1).body);
    }
    assert.equal(summary("created").active,false);
    assert.equal(summary("restarted").attempt,2);
    await open("created");
    assert.equal(el("send").disabled,true);
    assert.equal(el("restart-form").hidden,true);
    assert.equal(el("conversation").children[1].textContent,"Findings for created");
    await open("restarted");assert.equal(el("send").disabled,false);
  }
})().catch(error=>{console.error(error);process.exitCode=1;});
`
	for _, scenario := range []string{
		"success", "queued_input", "lost_response", "decode_failure", "server_failure", "changed_profile", "changed_text",
		"concurrent_submission", "selection_race", "detail_race", "reconnect", "reconnect_inflight", "different_subject", "namespace_switch",
		"refresh_failure", "lifecycle", "close_uncertain", "restart_uncertain", "message_reconnect", "message_subject",
		"status_preparing", "status_working", "status_completed", "status_failed", "status_cancelled", "archived_input", "archived_history",
	} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, node, "-e", harness, scenario)
			command.Stdin = strings.NewReader(string(script))
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("conversation interface: %v\n%s", err, output)
			}
		})
	}
}

func TestMessageInterfaceRetryAndContextIsolation(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is required for the interface event-handler test")
	}
	script, err := uiFiles.ReadFile("ui/app.js")
	if err != nil {
		t.Fatal(err)
	}
	const harness = `
const assert = require("node:assert/strict");
const vm = require("node:vm");
const source = require("node:fs").readFileSync(0, "utf8");
const scenario = process.argv[1];
class Element {
  constructor() { this.value = ""; this.textContent = ""; this.children = []; this.listeners = {}; this.disabled = false; }
  replaceChildren() { this.children = []; }
  append(...children) { this.children.push(...children); }
  addEventListener(type, handler) { this.listeners[type] = handler; }
}
const elements = new Map();
globalThis.document = {
  getElementById(id) { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); },
  createElement() { return new Element(); },
  createTextNode(text) { return {textContent:text}; },
};
globalThis.crypto = require("node:crypto");
globalThis.setInterval = () => 1;
globalThis.clearInterval = () => {};
const messages = [];
const accepted = new Map();
let sessionVersion = 1;
let first = true;
let release;
globalThis.fetch = async (path, options) => {
  if (path.endsWith("/messages")) {
    const key = options.headers["Idempotency-Key"];
    const text = JSON.parse(options.body).text;
    messages.push({path, key, text, auth:options.headers.Authorization});
    if (first && scenario === "definitive_rejection") {
      first = false;
      return {ok:false, status:400, text:async () => "Invalid text input"};
    }
    // Model the gateway contract: only the original key deduplicates acceptance.
    const identity = options.headers.Authorization + path + key;
    if (accepted.has(identity)) assert.equal(accepted.get(identity), text);
    else accepted.set(identity, text);
    if (first) {
      first = false;
      if (["lost_response", "session_switch", "reused_session_name", "changed_text"].includes(scenario)) throw new Error("lost response");
      if (scenario === "ambiguous_server_error") return {ok:false, status:503, text:async () => "Persistence unavailable"};
      if (scenario === "decode_failure") return {ok:true, json:async () => { throw new Error("truncated response"); }};
      if (["concurrent_submission", "switch_inflight", "reconnect"].includes(scenario)) await new Promise(resolve => { release = resolve; });
    }
    return {ok:true, json:async () => []};
  }
  let data;
  if (path.endsWith("/catalog")) data = {agents:[], environments:[], work_routes:[]};
  else if (path.endsWith("/sessions")) data = {sessions:[{name:"session-1", uid:"uid-1-" + sessionVersion}, {name:"session-2", uid:"uid-2"}]};
  else if (path.endsWith("/work")) data = {work:[]};
  else if (path.endsWith("/conversations")) data = {conversations:[]};
  else if (path.endsWith("/history")) data = {history:{status:"Ready", items:[]}};
  else if (path.endsWith("/actions")) data = {};
  else if (/\/sessions\/session-[12]$/.test(path)) data = {submissions:[]};
  else throw new Error("unexpected path " + path);
  return {ok:true, json:async () => data};
};
vm.runInThisContext(source);
(async () => {
  const el = document.getElementById;
  const fire = (id,type) => el(id).listeners[type]({preventDefault(){}});
  const connect = async (namespace,token) => {
    el("namespace").value = namespace;
    el("token").value = token;
    await fire("connect-form","submit");
  };
  const select = index => el("sessions").children[index].children[0].listeners.click();
  const submit = () => fire("message-form","submit");
  await connect("team", "test-first-principal");
  await select(0);
  el("message").value = "original message";
  if (["concurrent_submission", "switch_inflight", "reconnect"].includes(scenario)) {
    const pending = submit();
    assert.equal(typeof release, "function");
    if (scenario === "concurrent_submission") {
      assert.equal(el("send").disabled, true);
      assert.equal(el("message").disabled, true);
      await submit();
      assert.equal(messages.length, 1);
      release();
      await pending;
      assert.equal(el("send").disabled, false);
    } else {
      if (scenario === "reconnect") {
        await fire("disconnect", "click");
        assert.equal(el("message").value, "");
        await connect("other-team", "test-second-principal");
        assert.equal(el("send").disabled, true);
        await select(0);
      } else await select(1);
      assert.equal(el("message").value, "");
      el("message").value = "new context message";
      release();
      await pending;
      assert.equal(el("message").value, "new context message");
      assert.equal(el("error").textContent, "");
      await submit();
      assert.equal(messages.length, 2);
      assert.notEqual(messages[0].key, messages[1].key);
      assert.match(messages[1].path, scenario === "reconnect" ? /other-team\/sessions\/session-1\/messages$/ : /team\/sessions\/session-2\/messages$/);
      if (scenario === "reconnect") assert.equal(messages[1].auth, "Bearer test-second-principal");
    }
  } else {
    await submit();
    assert.notEqual(el("error").textContent, "");
    assert.equal(el("message").value, "original message");
    if (scenario === "definitive_rejection") {
      el("message").value = "corrected message";
      await submit();
      assert.notEqual(messages[0].key, messages[1].key);
      assert.equal(accepted.size, 1);
      return;
    }
    if (scenario === "reused_session_name") {
      sessionVersion++;
      await fire("refresh", "click");
      await select(0);
      assert.equal(el("message").value, "");
      el("message").value = "new Session message";
      await submit();
      assert.notEqual(messages[0].key, messages[1].key);
      return;
    }
    if (scenario === "session_switch") {
      await select(1);
      assert.equal(el("message").value, "");
      el("message").value = "second Session message";
      await submit();
      await select(0);
      assert.equal(el("message").value, "original message");
    }
    if (scenario === "changed_text") {
      el("message").value = "edited after uncertain acceptance";
      await submit();
      assert.equal(messages.length, 1);
      assert.match(el("error").textContent, /pending message/);
      assert.equal(el("message").value, "original message");
    }
    await submit();
    assert.equal(messages[0].key, messages.at(-1).key);
    assert.equal(accepted.size, scenario === "session_switch" ? 2 : 1);
    assert.equal(el("message").value, "");
    // A subsequent, explicitly submitted identical message is a new operation.
    el("message").value = "original message";
    await submit();
    assert.notEqual(messages[0].key, messages.at(-1).key);
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
`
	for _, scenario := range []string{
		"lost_response", "decode_failure", "ambiguous_server_error", "concurrent_submission",
		"session_switch", "reused_session_name", "switch_inflight", "reconnect", "definitive_rejection", "changed_text",
	} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, node, "-e", harness, scenario)
			command.Stdin = strings.NewReader(string(script))
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("message interface: %v\n%s", err, output)
			}
		})
	}
}

func TestConversationInterfacePagination(t *testing.T) {
	t.Parallel()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is required for the interface event-handler test")
	}
	script, err := uiFiles.ReadFile("ui/app.js")
	if err != nil {
		t.Fatal(err)
	}
	const harness = `
const assert = require("node:assert/strict");
const vm = require("node:vm");
const source = require("node:fs").readFileSync(0,"utf8");
class Element {
  constructor() {this.value="";this.textContent="";this.children=[];this.listeners={};}
  replaceChildren() {this.children=[];}
  append(...children) {this.children.push(...children);}
  addEventListener(type,handler) {this.listeners[type]=handler;}
}
const elements=new Map();
globalThis.document={
  getElementById(id) {if(!elements.has(id)) elements.set(id,new Element());return elements.get(id);},
  createElement() {return new Element();},createTextNode(text) {return {textContent:text};},
};
globalThis.crypto=require("node:crypto");
let poll;
globalThis.setInterval=callback=>{poll=callback;return 1;};
globalThis.clearInterval=()=>{};
let holdNext=false;
let release;
const collectionCalls=[];
const makeSummary=id=>({conversation_id:id,title:id,route_id:"profile",attempt:1,active:true,state:"open"});
globalThis.fetch=async(path,options)=>{
  assert.equal(options.headers.Authorization,"Bearer test-token");
  let data;
  if(path.endsWith("/catalog")) data={caller_subject:"alice",agents:[],environments:[],work_routes:[],more:true};
  else if(path.endsWith("/sessions")) data={sessions:[]};
  else if(path.endsWith("/work")) data={work:[]};
  else if(path.endsWith("/conversations")||path.includes("/conversations?")) {
    collectionCalls.push(path);
    const later=path.includes("?after=first");
    if(later&&holdNext) {holdNext=false;await new Promise(resolve=>{release=resolve;});}
    data={conversations:[makeSummary(later?"second":"first")],more:!later,next_cursor:later?"":"first"};
  } else if(path.endsWith("/conversations/second")) data={summary:makeSummary("second"),history:{items:[]},actions:{},submissions:[]};
  else throw new Error("unexpected path "+path);
  return {ok:true,json:async()=>data};
};
vm.runInThisContext(source);
(async()=>{
  const el=document.getElementById;
  const fire=(id,type)=>el(id).listeners[type]({preventDefault(){}});
  el("namespace").value="team";el("token").value="test-token";
  await fire("connect-form","submit");
  assert.equal(collectionCalls[0],"/v1/namespaces/team/conversations");
  assert.equal(el("conversation-first-page").disabled,true);
  assert.equal(el("conversation-next-page").disabled,false);
  assert.match(el("conversation-page-status").textContent,/more saved conversations/);
  assert.match(el("error").textContent,/Ask an administrator about omitted profiles/);
  holdNext=true;
  const pending=fire("conversation-next-page","click");
  assert.equal(typeof release,"function");
  assert.equal(el("conversation-next-page").disabled,true);
  await fire("conversation-first-page","click");
  release();await pending;
  assert.match(el("conversations").children[0].children[1].textContent,/first/);
  assert.equal(el("conversation-next-page").disabled,false);
  await fire("conversation-next-page","click");
  assert.equal(el("conversation-first-page").disabled,false);
  assert.equal(el("conversation-next-page").disabled,true);
  assert.match(el("conversation-page-status").textContent,/end of saved conversations/);
  assert.match(el("conversations").children[0].children[1].textContent,/second/);
  await poll();
  assert.match(collectionCalls.at(-1),/conversations\?after=first$/);
  await el("conversations").children[0].children[0].listeners.click();
  await poll();
  assert.match(collectionCalls.at(-1),/conversations\?after=first$/);
  assert.equal(el("selected-name").textContent,"second");
  await fire("conversation-first-page","click");
  assert.equal(collectionCalls.at(-1),"/v1/namespaces/team/conversations");
  await fire("disconnect","click");
  assert.equal(el("conversation-first-page").disabled,true);
  assert.equal(el("conversation-next-page").disabled,true);
})().catch(error=>{console.error(error);process.exitCode=1;});
`
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, "-e", harness)
	command.Stdin = strings.NewReader(string(script))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("conversation pagination: %v\n%s", err, output)
	}
}

func TestUnsupportedMessageContentIsRejectedBeforePersistence(t *testing.T) {
	store := testStore(t)
	p := simulatorForTest()
	g, _ := testGateway(t, store, p, testSession())
	response := request(g, http.MethodPost, _testMessagePath, _token, "image-1", `{"content":[{"type":"input_image","image_url":"https://example.com/image.png"}]}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unsupported input accepted: %d", response.Code)
	}
	record, err := store.Get(t.Context(), "uid-1")
	if err != nil || len(record.Submissions) != 0 || p.commits != 0 {
		t.Fatal("unsupported input was queued or submitted")
	}
}
