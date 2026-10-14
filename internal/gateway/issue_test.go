package gateway

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const _testIssueEnvironment = "readonly"

const _issueSecret = "fixture-webhook-secret-at-least-32-characters"

const _issueOpenedFixture = `{"action":"opened","repository":{"id":101,"full_name":"fixture/project"},"issue":{"id":501,"number":7,"title":" Investigate fixture failure ","body":"Reproduction: fixture input\r\nEvidence: assertion failed","state":"open","updated_at":"2026-10-09T08:00:00Z"},"sender":{"login":"fixture"}}`

type issueWriter struct {
	client.Client
	creates    int
	fail       error
	lose       bool
	deleteFail bool
	createRace func(client.Object) error
}

func (w *issueWriter) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	w.creates++
	if w.createRace != nil {
		return w.createRace(obj)
	}
	if w.fail != nil {
		return w.fail
	}
	obj.SetUID(types.UID("issue-session-uid"))
	if err := w.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	if w.lose {
		w.lose = false
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (w *issueWriter) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if w.deleteFail {
		return io.ErrUnexpectedEOF
	}
	return w.Client.Delete(ctx, obj, opts...)
}

func issueFixture(t *testing.T) []byte {
	t.Helper()
	return []byte(_issueOpenedFixture)
}
func issueTestGateway(t *testing.T, store Storage) (*Gateway, *issueWriter) {
	t.Helper()
	env := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Namespace: _testNamespace, Name: _testIssueEnvironment}, Spec: agentsv1alpha1.EnvironmentSpec{Mode: _noneMode}}
	g, k8s := testGateway(t, store, simulatorForTest(), env)
	writer := &issueWriter{Client: k8s}
	if err := g.EnableIssues(writer, []IssueRoute{{ID: "project", Workflow: _issueTextOnlyWorkflow, Namespace: _testNamespace, RepositoryID: 101, Secret: _issueSecret, Agent: "investigator", Environment: env.Name, Vaults: []string{"evidence"}, SpendControl: &IssueSpendControl{Mode: "unlimited"}}}); err != nil {
		t.Fatal(err)
	}
	return g, writer
}
func signedIssue(g *Gateway, raw []byte, delivery string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/issue-events/github/project", strings.NewReader(string(raw)))
	mac := hmac.New(sha256.New, []byte(_issueSecret))
	_, _ = mac.Write(raw)
	req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	req.Header.Set("X-GitHub-Event", "issues")
	req.Header.Set("X-GitHub-Delivery", delivery)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)
	return w
}
func sourceFromResponse(t *testing.T, response *httptest.ResponseRecorder) IssueSource {
	t.Helper()
	if response.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", response.Code, response.Body.String())
	}
	var source IssueSource
	if err := json.Unmarshal(response.Body.Bytes(), &source); err != nil {
		t.Fatal(err)
	}
	return source
}
func readIssue(t *testing.T, store Storage, id string) Record {
	t.Helper()
	record, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestIssueAuthenticationNormalizationAndRouting(t *testing.T) {
	g, _ := issueTestGateway(t, testStore(t))
	raw := issueFixture(t)
	if response := request(g, http.MethodPost, "/v1/issue-events/github/project", _token, "", string(raw)); response.Code != http.StatusUnauthorized {
		t.Fatal(response.Code)
	}
	tests := []struct {
		name string
		body []byte
		want int
	}{
		{name: "valid", body: raw, want: http.StatusAccepted},
		{name: "wrong repository", body: []byte(strings.ReplaceAll(string(raw), `"id":101`, `"id":102`)), want: http.StatusBadRequest},
		{name: "unsupported action", body: []byte(strings.ReplaceAll(string(raw), `"opened"`, `"labeled"`)), want: http.StatusBadRequest},
		{name: "invalid UTF8", body: append(append([]byte(nil), raw...), 0xff), want: http.StatusBadRequest},
		{name: "invalid JSON", body: []byte(`{`), want: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := signedIssue(g, tt.body, "test-"+strings.ReplaceAll(tt.name, " ", "-")).Code; got != tt.want {
				t.Fatal(got)
			}
		})
	}
	records, err := g.store.Records(context.Background())
	if err != nil || len(records) != 1 {
		t.Fatal(err, len(records))
	}
	source := records[0].Issue
	if source.Workflow != _issueTextOnlyWorkflow || source.StableID != "github/repositories/101/issues/501" || source.Deliveries[0].Evidence.Title != "Investigate fixture failure" || strings.Contains(source.Deliveries[0].Evidence.Body, "\r") {
		t.Fatal(source)
	}
	if source.Policy.AgentRef.Name != "investigator" || source.Policy.EnvironmentRef.Name != "readonly" || source.Policy.VaultRefs[0].Name != "evidence" || source.Policy.Agent.Tools == nil || len(*source.Policy.Agent.Tools) != 0 || source.Policy.SpendControl == nil || source.Policy.SpendControl.Mode != "unlimited" {
		t.Fatal("policy not frozen")
	}
	if strings.Contains(string(mustJSON(t, source)), _issueSecret) {
		t.Fatal("webhook secret persisted")
	}
	path := "/v1/namespaces/team/sources/" + source.ID
	if got := request(g, http.MethodGet, path, "invalid", "", "").Code; got != http.StatusUnauthorized {
		t.Fatal(got)
	}
	if got := request(g, http.MethodGet, strings.Replace(path, "/team/", "/other/", 1), _token, "", "").Code; got != http.StatusNotFound {
		t.Fatal(got)
	}
}
func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestIssueDuplicateChangedDeliveryAndConcurrentReceipt(t *testing.T) {
	store := testStore(t)
	g, writer := issueTestGateway(t, store)
	raw := issueFixture(t)
	source := sourceFromResponse(t, signedIssue(g, raw, "delivery-1"))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if got := signedIssue(g, raw, "delivery-1").Code; got != http.StatusAccepted {
				t.Error(got)
			}
		})
	}
	wg.Wait()
	changed := []byte(strings.ReplaceAll(string(raw), `"opened"`, `"edited"`))
	if got := signedIssue(g, changed, "delivery-1").Code; got != http.StatusConflict {
		t.Fatal(got)
	}
	_ = sourceFromResponse(t, signedIssue(g, changed, "delivery-2"))
	other := []byte(strings.ReplaceAll(string(raw), `"id":501`, `"id":502`))
	if got := signedIssue(g, other, "delivery-1").Code; got != http.StatusConflict {
		t.Fatal(got)
	}
	for range 3 {
		if err := g.syncIssue(context.Background(), source.ID); err != nil {
			t.Fatal(err)
		}
	}
	record := readIssue(t, store, source.ID)
	conversation := readIssue(t, store, record.Issue.SessionUID)
	if writer.creates != 1 || len(record.Issue.Deliveries) != 2 || len(conversation.Submissions) != 1 || conversation.InitialKey != _issueInitialKey {
		t.Fatal("duplicate Session or input")
	}
}

func TestIssueRestartAfterCreateAndEnqueueBeforeAcknowledgement(t *testing.T) {
	store, dsn := postgresForTest(t, 32)
	g, writer := issueTestGateway(t, store)
	source := sourceFromResponse(t, signedIssue(g, issueFixture(t), "delivery-1"))
	writer.lose = true
	if err := g.syncIssue(context.Background(), source.ID); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenPostgreSQL(context.Background(), PostgreSQLConfig{DSN: dsn, AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	g.store = store
	if err := g.syncIssue(context.Background(), source.ID); err != nil {
		t.Fatal(err)
	}
	// Simulate stopping after enqueue committed but before source acknowledgement.
	if err := store.Update(t.Context(), source.ID, func(r *Record) error { r.Issue.InputQueued = false; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := g.syncIssue(context.Background(), source.ID); err != nil {
		t.Fatal(err)
	}
	record := readIssue(t, store, source.ID)
	conversation := readIssue(t, store, record.Issue.SessionUID)
	if writer.creates != 1 || len(conversation.Submissions) != 1 {
		t.Fatal("restart duplicated intent")
	}
	if got := signedIssue(g, issueFixture(t), "delivery-1").Code; got != http.StatusAccepted {
		t.Fatal(got)
	}
}

func TestIssueCreationRejectionUnknownOutcomeAndOwnership(t *testing.T) {
	tests := []struct {
		name      string
		failure   error
		wantClaim bool
	}{
		{name: "definitive rejection", failure: apierrors.NewForbidden(schema.GroupResource{Resource: "sessions"}, "issue", errors.New("denied"))},
		{name: "transport unknown", failure: io.ErrUnexpectedEOF, wantClaim: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := testStore(t)
			g, writer := issueTestGateway(t, store)
			source := sourceFromResponse(t, signedIssue(g, issueFixture(t), "delivery-1"))
			writer.fail = tt.failure
			if err := g.syncIssue(context.Background(), source.ID); !errors.Is(err, tt.failure) {
				t.Fatal(err)
			}
			if got := readIssue(t, store, source.ID).Issue.CreationClaimed; got != tt.wantClaim {
				t.Fatal(got)
			}
			writer.fail = nil
			err := g.syncIssue(context.Background(), source.ID)
			if tt.wantClaim {
				if err == nil || writer.creates != 1 {
					t.Fatal("ambiguous create repeated")
				}
				return
			}
			if err != nil || writer.creates != 2 {
				t.Fatal(err, writer.creates)
			}
		})
	}
	store := testStore(t)
	g, writer := issueTestGateway(t, store)
	source := sourceFromResponse(t, signedIssue(g, issueFixture(t), "delivery-1"))
	record := readIssue(t, store, source.ID)
	foreign := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: record.Name, Namespace: record.Namespace, UID: "foreign"}, Spec: record.Issue.Policy}
	if err := writer.Client.Create(context.Background(), foreign); err != nil {
		t.Fatal(err)
	}
	if err := g.syncIssue(context.Background(), source.ID); err == nil || writer.creates != 0 {
		t.Fatal("foreign Session adopted")
	}
}

func TestIssueCleanupRetryEvidenceRetentionAndNoRecreation(t *testing.T) {
	store := testStore(t)
	g, writer := issueTestGateway(t, store)
	source := sourceFromResponse(t, signedIssue(g, issueFixture(t), "delivery-1"))
	if err := g.syncIssue(context.Background(), source.ID); err != nil {
		t.Fatal(err)
	}
	path := "/v1/namespaces/team/sources/" + source.ID
	if got := request(g, http.MethodPost, path+"/close", _token, "", "").Code; got != http.StatusAccepted {
		t.Fatal(got)
	}
	record := readIssue(t, store, source.ID)
	if got := request(g, http.MethodPost, "/v1/namespaces/team/sessions/"+record.Name+"/messages", _token, "after-close", `{"text":"must not send"}`).Code; got != http.StatusGone {
		t.Fatal("close did not fence follow-up", got)
	}
	writer.deleteFail = true
	if err := g.syncIssue(context.Background(), source.ID); err == nil || readIssue(t, store, source.ID).Issue.Cleaned {
		t.Fatal("cleanup failure concealed")
	}
	writer.deleteFail = false
	for range 2 {
		if err := g.syncIssue(context.Background(), source.ID); err != nil {
			t.Fatal(err)
		}
	}
	if !readIssue(t, store, source.ID).Issue.Cleaned {
		t.Fatal("cleanup not observed")
	}
	_ = sourceFromResponse(t, signedIssue(g, issueFixture(t), "delivery-1"))
	if err := g.syncIssue(context.Background(), source.ID); err != nil || writer.creates != 1 {
		t.Fatal("closed investigation recreated")
	}
	response := request(g, http.MethodGet, path, _token, "", "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "assertion failed") || !strings.Contains(response.Body.String(), "issue-initial") {
		t.Fatal("audit unavailable after cleanup")
	}
}

func TestIssuePolicyBlocksEnvironmentOrSpecChange(t *testing.T) {
	store := testStore(t)
	g, writer := issueTestGateway(t, store)
	source := sourceFromResponse(t, signedIssue(g, issueFixture(t), "delivery-1"))
	if err := g.syncIssue(context.Background(), source.ID); err != nil {
		t.Fatal(err)
	}
	record := readIssue(t, store, source.ID)
	var session agentsv1alpha1.Session
	if err := writer.Get(context.Background(), client.ObjectKey{Namespace: record.Namespace, Name: record.Name}, &session); err != nil {
		t.Fatal(err)
	}
	env := &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: _noneMode}}
	if err := g.ValidateInvestigation(context.Background(), &session, env); err != nil {
		t.Fatal(err)
	}
	env.Profile.Mode = "selfHosted"
	if err := g.ValidateInvestigation(context.Background(), &session, env); err == nil {
		t.Fatal("compute escalation allowed")
	}
	env.Profile.Mode = _noneMode
	session.Spec.Agent.MultiAgent.Enabled = true
	if err := g.ValidateInvestigation(context.Background(), &session, env); err == nil {
		t.Fatal("policy escalation allowed")
	}
}

func TestExternalWriteRegistrationRequiresDownstreamDeduplication(t *testing.T) {
	store := testStore(t)
	g, _ := issueTestGateway(t, store)
	functions := map[string]Function{_testExternal: {Consequential: true, Execute: func(context.Context, Operation) (string, error) { return "", nil }}}
	if _, err := New(store, g.reader, g.provider, []Principal{{Token: _token, Subject: "caller", Namespace: _testNamespace}}, functions); err == nil {
		t.Fatal("unsafe external handler registered")
	}
	function := functions[_testExternal]
	function.DownstreamDeduplicates = true
	functions[_testExternal] = function
	if _, err := New(store, g.reader, g.provider, []Principal{{Token: _token, Subject: "caller", Namespace: _testNamespace}}, functions); err != nil {
		t.Fatal(err)
	}
}

func TestIssueReservesConversationCapacityBeforeAcceptance(t *testing.T) {
	store, _ := postgresForTestLimits(t, 32, 32)
	for i := range 32 - 2 {
		if err := store.Update(t.Context(), string(rune('a'+i)), func(*Record) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	g, _ := issueTestGateway(t, store)
	source := sourceFromResponse(t, signedIssue(g, issueFixture(t), "delivery-1"))
	if err := store.Update(t.Context(), "standalone", func(*Record) error { return nil }); err == nil {
		t.Fatal("pending conversation slot consumed")
	}
	if err := g.syncIssue(context.Background(), source.ID); err != nil {
		t.Fatal(err)
	}
	other := []byte(strings.ReplaceAll(string(issueFixture(t)), `"id":501`, `"id":502`))
	if got := signedIssue(g, other, "delivery-2").Code; got != http.StatusServiceUnavailable {
		t.Fatal(got)
	}
}

func TestIssuePendingInitialRejectsFollowupAndReservesInitialKey(t *testing.T) {
	store := testStore(t)
	g, _ := issueTestGateway(t, store)
	source := sourceFromResponse(t, signedIssue(g, issueFixture(t), "delivery-1"))
	if err := g.syncIssue(context.Background(), source.ID); err != nil {
		t.Fatal(err)
	}
	record := readIssue(t, store, source.ID)
	path := "/v1/namespaces/team/sessions/" + record.Name + "/messages"
	if err := store.Update(t.Context(), source.ID, func(r *Record) error { r.Issue.InputQueued = false; return nil }); err != nil {
		t.Fatal(err)
	}
	if got := request(g, http.MethodPost, path, _token, "followup", `{"text":"wait for initial"}`).Code; got != http.StatusServiceUnavailable {
		t.Fatal(got)
	}
	if err := g.syncIssue(context.Background(), source.ID); err != nil {
		t.Fatal(err)
	}
	if got := request(g, http.MethodPost, path, _token, _issueInitialKey, `{"text":"must not replace initial"}`).Code; got != http.StatusBadRequest {
		t.Fatal(got)
	}
	if len(readIssue(t, store, record.Issue.SessionUID).Submissions) != 1 {
		t.Fatal("initial message overwritten")
	}
}

func TestIssueDoesNotLinkOrDeleteRecreatedSession(t *testing.T) {
	store := testStore(t)
	g, writer := issueTestGateway(t, store)
	source := sourceFromResponse(t, signedIssue(g, issueFixture(t), "delivery-1"))
	if err := g.syncIssue(context.Background(), source.ID); err != nil {
		t.Fatal(err)
	}
	record := readIssue(t, store, source.ID)
	var session agentsv1alpha1.Session
	key := client.ObjectKey{Namespace: record.Namespace, Name: record.Name}
	if err := writer.Get(context.Background(), key, &session); err != nil {
		t.Fatal(err)
	}
	if err := writer.Client.Delete(context.Background(), &session); err != nil {
		t.Fatal(err)
	}
	foreign := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: "replacement-uid"}, Spec: record.Issue.Policy}
	if err := writer.Client.Create(context.Background(), foreign); err != nil {
		t.Fatal(err)
	}
	path := "/v1/namespaces/team/sources/" + source.ID
	response := request(g, http.MethodGet, path, _token, "", "")
	var view struct {
		URL string `json:"session_url"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &view) != nil || view.URL != "" {
		t.Fatal("linked a foreign replacement")
	}
	if got := request(g, http.MethodPost, path+"/close", _token, "", "").Code; got != http.StatusAccepted {
		t.Fatal(got)
	}
	if err := g.syncIssue(context.Background(), source.ID); err == nil {
		t.Fatal("replacement adopted or deleted")
	}
	if err := writer.Get(context.Background(), key, &session); err != nil || session.UID != foreign.UID {
		t.Fatal("foreign replacement removed", err)
	}
}

func TestIssueMarkerRemovalCannotBypassReadOnlyPolicy(t *testing.T) {
	store := testStore(t)
	g, writer := issueTestGateway(t, store)
	source := sourceFromResponse(t, signedIssue(g, issueFixture(t), "delivery-1"))
	if err := g.syncIssue(context.Background(), source.ID); err != nil {
		t.Fatal(err)
	}
	record := readIssue(t, store, source.ID)
	var session agentsv1alpha1.Session
	if err := writer.Get(context.Background(), client.ObjectKey{Namespace: record.Namespace, Name: record.Name}, &session); err != nil {
		t.Fatal(err)
	}
	session.Annotations = nil
	if err := writer.Update(context.Background(), &session); err != nil {
		t.Fatal(err)
	}
	env := &agentsv1alpha1.SessionEnvironmentSnapshot{Profile: agentsv1alpha1.EnvironmentSpec{Mode: _noneMode}}
	if err := g.ValidateInvestigation(context.Background(), &session, env); err == nil {
		t.Fatal("marker removal bypassed creation policy")
	}
	if err := g.investigationOpen(t.Context(), &session); err == nil {
		t.Fatal("marker removal bypassed input policy")
	}
	if got := request(g, http.MethodPost, "/v1/namespaces/team/sessions/"+record.Name+"/messages", _token, "followup", `{"text":"must be blocked"}`).Code; got != http.StatusServiceUnavailable {
		t.Fatal(got)
	}
}

func TestIssueRejectsExpandedInputBeforeAcknowledgement(t *testing.T) {
	g, _ := issueTestGateway(t, testStore(t))
	var payload map[string]any
	if err := json.Unmarshal(issueFixture(t), &payload); err != nil {
		t.Fatal(err)
	}
	issue, ok := payload["issue"].(map[string]any)
	if !ok {
		t.Fatal("invalid fixture")
	}
	issue["body"] = strings.Repeat("<", 100<<10)
	// GitHub may use unescaped HTML characters in the signed JSON body.
	var body strings.Builder
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		t.Fatal(err)
	}
	if body.Len() > _maxInputBytes {
		t.Fatal("fixture exceeds raw ingress limit")
	}
	if got := signedIssue(g, []byte(body.String()), "expanded-delivery").Code; got != http.StatusBadRequest {
		t.Fatal(got)
	}
	records, err := g.store.Records(context.Background())
	if err != nil || len(records) != 0 {
		t.Fatal("unsupported input acknowledged", err)
	}
}

func TestIssueRouteParserRejectsUnsupportedPolicyFields(t *testing.T) {
	valid := `[{"id":"project","workflow":"issue_text_only_v1","namespace":"team","repository_id":101,"secret":"fixture-secret-32-characters-long","agent":"investigator","environment":"readonly","spend_control":{"mode":"unlimited"}}]`
	tests := []struct {
		name      string
		give      string
		wantError bool
	}{
		{name: "valid", give: valid},
		{name: "missing workflow", give: strings.Replace(valid, `"workflow":"issue_text_only_v1",`, "", 1), wantError: true},
		{name: "unsupported workflow", give: strings.Replace(valid, `"issue_text_only_v1"`, `"repository_investigation"`, 1), wantError: true},
		{name: "unknown policy field", give: strings.Replace(valid, `"id":"project"`, `"id":"project","vaultRefs":["lost"]`, 1), wantError: true},
		{name: "missing spend control", give: strings.Replace(valid, `,"spend_control":{"mode":"unlimited"}`, "", 1), wantError: true},
		{name: "invalid spend control", give: strings.Replace(valid, `"mode":"unlimited"`, `"mode":"limited","limit_cents":0`, 1), wantError: true},
		{name: "unknown spend field", give: strings.Replace(valid, `"mode":"unlimited"`, `"mode":"unlimited","limit":100`, 1), wantError: true},
		{name: "trailing JSON", give: valid + `{}`, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			routes, err := ParseIssueRoutes([]byte(tt.give))
			if (err != nil) != tt.wantError {
				t.Fatal(err)
			}
			if !tt.wantError && len(routes) != 1 {
				t.Fatal("route lost")
			}
			if err != nil && strings.Contains(err.Error(), "fixture-secret") {
				t.Fatal("configuration error exposed credentials")
			}
		})
	}
}

func TestIssueRouteRequiresExplicitWorkflow(t *testing.T) {
	g, writer := issueTestGateway(t, testStore(t))
	route := g.issueRoutes["project"]
	route.Workflow = ""
	if err := g.EnableIssues(writer, []IssueRoute{route}); err == nil {
		t.Fatal("Issue route without a workflow was accepted")
	}
	route.Workflow = "repository_investigation"
	if err := g.EnableIssues(writer, []IssueRoute{route}); err == nil {
		t.Fatal("Unsupported issue workflow was accepted")
	}
}

func TestIssueInitialKeepsNoneRoutingWhileProfileIsBlocked(t *testing.T) {
	store := testStore(t)
	g, writer := issueTestGateway(t, store)
	var env agentsv1alpha1.Environment
	key := client.ObjectKey{Namespace: _testNamespace, Name: _testIssueEnvironment}
	if err := writer.Get(context.Background(), key, &env); err != nil {
		t.Fatal(err)
	}
	env.Spec.Mode = "openaiHosted"
	if err := writer.Update(context.Background(), &env); err != nil {
		t.Fatal(err)
	}
	source := sourceFromResponse(t, signedIssue(g, issueFixture(t), "delivery-1"))
	if err := g.syncIssue(context.Background(), source.ID); err != nil {
		t.Fatal(err)
	}
	record := readIssue(t, store, source.ID)
	conversation := readIssue(t, store, record.Issue.SessionUID)
	if len(conversation.Submissions) != 1 || !conversation.Submissions[0].Initial {
		t.Fatal("blocked profile lost initial routing")
	}
	env.Spec.Mode = _noneMode
	if err := writer.Update(context.Background(), &env); err != nil {
		t.Fatal(err)
	}
	text, keyValue, err := g.InitialInput(context.Background(), record.Issue.SessionUID)
	if err != nil || text == "" || keyValue == "" {
		t.Fatal("first input not recoverable", err)
	}
}

func TestIssueAlreadyExistsDoesNotAdoptAMatchingForeignMarker(t *testing.T) {
	store := testStore(t)
	g, writer := issueTestGateway(t, store)
	source := sourceFromResponse(t, signedIssue(g, issueFixture(t), "delivery-1"))
	writer.createRace = func(obj client.Object) error {
		foreign, ok := obj.DeepCopyObject().(*agentsv1alpha1.Session)
		if !ok {
			return errors.New("invalid fixture object")
		}
		foreign.UID = "foreign-race-uid"
		if err := writer.Client.Create(context.Background(), foreign); err != nil {
			return err
		}
		return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "sessions"}, foreign.Name)
	}
	if err := g.syncIssue(context.Background(), source.ID); !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	writer.createRace = nil
	if err := g.syncIssue(context.Background(), source.ID); err == nil {
		t.Fatal("foreign object adopted after definitive rejection")
	}
	record := readIssue(t, store, source.ID)
	if record.Issue.CreationClaimed || record.Issue.SessionUID != "" || record.Issue.InputQueued {
		t.Fatal("foreign object became source owner")
	}
}
