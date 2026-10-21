package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	_profileTestName           = "payments"
	_profileTestBody           = `{"route_id":"payments","text":"Inspect the repository"}`
	_profileTestAgent          = "profile-agent"
	_profileTestVault          = "profile-vault"
	_testForeignUID            = "foreign"
	_testObservedReason        = "Observed"
	_profileTestVaultUID       = "vault-profile-uid"
	_profileTestEnvironment    = "profile-environment"
	_profileTestEnvironmentUID = "environment-profile-uid"
)

func profileReadyConditions(generation int64) []metav1.Condition {
	return []metav1.Condition{{Type: _readyCondition, Status: metav1.ConditionTrue,
		Reason: _testObservedReason, ObservedGeneration: generation}}
}

func readyProfileObjects(namespace string) (*agentsv1alpha1.Profile, []client.Object) {
	agent := &agentsv1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: _profileTestAgent, Namespace: namespace, UID: "agent-profile-uid", Generation: 1},
		Status: agentsv1alpha1.AgentStatus{Conditions: profileReadyConditions(1)}}
	environment := &agentsv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: _profileTestEnvironment, Namespace: namespace, UID: _profileTestEnvironmentUID, Generation: 1},
		Spec: agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeNone}, Status: agentsv1alpha1.EnvironmentStatus{Conditions: profileReadyConditions(1)}}
	vault := &agentsv1alpha1.Vault{ObjectMeta: metav1.ObjectMeta{Name: _profileTestVault, Namespace: namespace, UID: _profileTestVaultUID, Generation: 1},
		Status: agentsv1alpha1.VaultStatus{Conditions: profileReadyConditions(1)}}
	profile := &agentsv1alpha1.Profile{ObjectMeta: metav1.ObjectMeta{Name: _profileTestName, Namespace: namespace, UID: types.UID("profile-" + namespace), Generation: 1, ResourceVersion: "1"},
		Spec: agentsv1alpha1.ProfileSpec{AgentRef: agentsv1alpha1.AgentReference{Name: agent.Name},
			EnvironmentRef: agentsv1alpha1.AgentReference{Name: environment.Name}, VaultRefs: []agentsv1alpha1.AgentReference{{Name: vault.Name}},
			SpendControl: agentsv1alpha1.SessionSpendControl{Mode: _testWorkUnlimited}},
		Status: agentsv1alpha1.ProfileStatus{ObservedGeneration: 1, Conditions: profileReadyConditions(1),
			Agent:       &agentsv1alpha1.ProfileDependencyStatus{Name: agent.Name, UID: string(agent.UID), Generation: 1},
			Environment: &agentsv1alpha1.ProfileDependencyStatus{Name: environment.Name, UID: string(environment.UID), Generation: 1},
			Vaults:      []agentsv1alpha1.ProfileDependencyStatus{{Name: vault.Name, UID: string(vault.UID), Generation: 1}}}}
	profile.Status.PolicyDigest = agentsv1alpha1.ProfilePolicyDigest(profile.Spec)
	return profile, []client.Object{agent, environment, vault, profile}
}

func profileGateway(t *testing.T, store Storage) (*Gateway, *sessionWriter, *agentsv1alpha1.Profile) {
	t.Helper()
	profile, objects := readyProfileObjects(_testNamespace)
	g, k8s := testGateway(t, store, simulatorForTest(), objects...)
	writer := &sessionWriter{Client: k8s}
	if err := g.EnableProfiles(writer); err != nil {
		t.Fatal(err)
	}
	return g, writer, profile
}

func TestProfileAdmissionLiveEditNamespaceAndRetirement(t *testing.T) {
	store := testStore(t)
	g, writer, profile := profileGateway(t, store)
	first := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "profile-first", _profileTestBody))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "profile-first")
	original := readWorkRecord(t, store, id)
	if original.Version != _recordVersion || original.Work.Attempts[0].Profile.UID != string(profile.UID) || writer.creates != 0 {
		t.Fatal("Profile acceptance did not freeze safe provenance before provisioning")
	}
	otherProfile, objects := readyProfileObjects(_testWorkOther)
	for _, object := range objects {
		object.SetResourceVersion("")
		if err := writer.Client.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	other, _, err := g.createConversation(t.Context(), requestScope{Namespace: _testWorkOther, Subject: _testConversationSubject}, "profile-first",
		ConversationInput{RouteID: _profileTestName, Text: "Inspect the repository"})
	if err != nil || other.Work.Attempts[0].Profile.UID != string(otherProfile.UID) || other.UID == original.UID {
		t.Fatal("same-named Profile crossed namespaces", err)
	}
	var current agentsv1alpha1.Profile
	if err := writer.Get(t.Context(), client.ObjectKeyFromObject(profile), &current); err != nil {
		t.Fatal(err)
	}
	limit := int64(150)
	current.Spec.SpendControl = agentsv1alpha1.SessionSpendControl{Mode: _scenarioLimited, LimitCents: &limit}
	current.Generation++
	current.Status.ObservedGeneration = current.Generation
	current.Status.Conditions = profileReadyConditions(current.Generation)
	current.Status.PolicyDigest = agentsv1alpha1.ProfilePolicyDigest(current.Spec)
	if err := writer.Update(t.Context(), &current); err != nil {
		t.Fatal(err)
	}
	second := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "profile-second", _profileTestBody))
	secondRecord, _, err := g.lookupConversation(t.Context(), _testNamespace, second.ConversationID)
	if err != nil || secondRecord.Work.Attempts[0].Profile.Generation != 2 ||
		secondRecord.Work.Attempts[0].Policy.SpendControl.Mode != _scenarioLimited {
		t.Fatal("running gateway did not discover Profile edit", err)
	}
	if replay := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "profile-first", _profileTestBody)); !reflect.DeepEqual(first, replay) {
		t.Fatal("Profile edit changed original receipt")
	}
	if err := writer.Delete(t.Context(), &current); err != nil {
		t.Fatal(err)
	}
	if replay := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "profile-first", _profileTestBody)); !reflect.DeepEqual(first, replay) {
		t.Fatal("Profile retirement changed original receipt")
	}
	if response := conversationRequest(g, http.MethodPost, _conversationPath, "profile-retired", _profileTestBody); response.Code != http.StatusNotFound {
		t.Fatal("retired Profile admitted new work", response.Code)
	}
	readConversationReceipt(t, conversationRequest(g, http.MethodPost, first.ConversationURL+"/messages", "retired-continue", `{"text":"Continue accepted work"}`))
	readConversationReceipt(t, conversationRequest(g, http.MethodPost, first.ConversationURL+"/close", "retired-close", `{}`))
	if err := store.Update(t.Context(), id, func(r *Record) error {
		r.Work.State = _workClosed
		r.Work.Attempts[0].Cleaned = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if response := conversationRequest(g, http.MethodPost, first.ConversationURL+"/restart", "retired-restart", `{"text":"Fresh"}`); response.Code != http.StatusNotFound {
		t.Fatal("retired Profile restarted work", response.Code)
	}
	current.UID, current.ResourceVersion = "profile-recreated", ""
	if err := writer.Client.Create(t.Context(), &current); err != nil {
		t.Fatal(err)
	}
	restarted := readConversationReceipt(t, conversationRequest(g, http.MethodPost, first.ConversationURL+"/restart", "retired-restart", `{"text":"Fresh"}`))
	_, attempt, err := g.lookupConversation(t.Context(), _testNamespace, restarted.ConversationID)
	if err != nil || attempt.Profile.UID != "profile-recreated" || attempt.Profile.Generation != 2 {
		t.Fatal("new attempt did not use current Profile identity", err)
	}
	if !reflect.DeepEqual(readWorkRecord(t, store, id).Work.Attempts[0].Profile, original.Work.Attempts[0].Profile) {
		t.Fatal("Profile recreation altered old attempt")
	}
}

func TestProfileAdmissionRejectsStaleAndReplacedDependencies(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*agentsv1alpha1.Profile, []client.Object)
	}{
		{name: "stale profile", mutate: func(p *agentsv1alpha1.Profile, _ []client.Object) { p.Generation++ }},
		{name: "wrong policy digest", mutate: func(p *agentsv1alpha1.Profile, _ []client.Object) { p.Status.PolicyDigest = "wrong" }},
		{name: "invalid spend", mutate: func(p *agentsv1alpha1.Profile, _ []client.Object) { p.Spec.SpendControl.Mode = "" }},
		{name: "agent recreated", mutate: func(_ *agentsv1alpha1.Profile, objects []client.Object) { objects[0].SetUID("replacement") }},
		{name: "environment stale", mutate: func(_ *agentsv1alpha1.Profile, objects []client.Object) { objects[1].SetGeneration(2) }},
		{name: "vault recreated", mutate: func(_ *agentsv1alpha1.Profile, objects []client.Object) { objects[2].SetUID("replacement") }},
		{name: "namespace selector", mutate: func(p *agentsv1alpha1.Profile, _ []client.Object) { p.Spec.AgentRef.Name = "other/agent" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			profile, objects := readyProfileObjects(_testNamespace)
			tt.mutate(profile, objects)
			store := testStore(t)
			provider := simulatorForTest()
			g, writer := testGateway(t, store, provider, objects...)
			if err := g.EnableProfiles(writer); err != nil {
				t.Fatal(err)
			}
			response := conversationRequest(g, http.MethodPost, _conversationPath, "unready", _profileTestBody)
			records, err := store.Records(t.Context())
			if response.Code != http.StatusServiceUnavailable || err != nil || len(records) != 0 || provider.posts != 0 {
				t.Fatal("unready Profile consumed capacity or performed provider I/O", response.Code, err)
			}
		})
	}
}

func TestProfileSelectionIsCopiedAndResolvedUIDsArePinned(t *testing.T) {
	store := testStore(t)
	g, writer, profile := profileGateway(t, store)
	selection, err := g.selectProfile(t.Context(), _testNamespace, profile.Name)
	if err != nil {
		t.Fatal(err)
	}
	selection.route.Vaults[0] = "modified"
	selection.provenance.Vaults[0].UID = "modified"
	fresh, err := g.selectProfile(t.Context(), _testNamespace, profile.Name)
	if err != nil || fresh.route.Vaults[0] != _profileTestVault || fresh.provenance.Vaults[0].UID != _profileTestVaultUID {
		t.Fatal("selection retained caller mutations", err)
	}
	receipt := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "pinned", _profileTestBody))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "pinned")
	if err := g.syncWork(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	record := readWorkRecord(t, store, id)
	session := workerWorkSession(t, writer, record)
	environment := &agentsv1alpha1.SessionEnvironmentSnapshot{Name: _profileTestEnvironment, UID: _profileTestEnvironmentUID, Profile: agentsv1alpha1.EnvironmentSpec{Mode: agentsv1alpha1.EnvironmentModeNone}}
	if err := g.validateProfileDependencies(t.Context(), session, environment, record.Work.Attempts[0].Profile); err != nil {
		t.Fatal(err)
	}
	var agent agentsv1alpha1.Agent
	if err := writer.Get(t.Context(), client.ObjectKey{Namespace: _testNamespace, Name: _profileTestAgent}, &agent); err != nil {
		t.Fatal(err)
	}
	if err := writer.Delete(t.Context(), &agent); err != nil {
		t.Fatal(err)
	}
	agent.UID, agent.ResourceVersion = "replacement-agent", ""
	if err := writer.Client.Create(t.Context(), &agent); err != nil {
		t.Fatal(err)
	}
	if err := g.validateProfileDependencies(t.Context(), session, environment, record.Work.Attempts[0].Profile); err == nil {
		t.Fatal("accepted input adopted a same-named replacement dependency")
	}
	// The second controller check compares actual resolution, even if a later
	// reread could once again return the original dependency identity.
	session.Status.AgentSnapshot = &agentsv1alpha1.SessionAgentSnapshot{Name: _profileTestAgent, UID: "replacement-agent"}
	if err := g.validateProfileDependencies(t.Context(), session, environment, record.Work.Attempts[0].Profile); err == nil {
		t.Fatal("actual resolved replacement passed the pre-claim gate")
	}
	session.Status.AgentSnapshot.UID = "agent-profile-uid"
	session.Status.VaultSnapshots = []agentsv1alpha1.SessionVaultSnapshot{{Name: _profileTestVault, UID: _profileTestVaultUID}}
	if err := g.validateProfileDependencies(t.Context(), session, environment, record.Work.Attempts[0].Profile); err != nil {
		t.Fatal("frozen original snapshots depended on today's mutable sources", err)
	}
	if response := conversationRequest(g, http.MethodGet, receipt.ConversationURL, "", ""); response.Code != http.StatusOK {
		t.Fatal("retained Profile conversation became unreadable")
	}
}

type profilePageReader struct{ client.Reader }

func (r *profilePageReader) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	profiles, ok := list.(*agentsv1alpha1.ProfileList)
	if !ok {
		return r.Reader.List(ctx, list, options...)
	}
	var opts client.ListOptions
	opts.ApplyOptions(options)
	if err := r.Reader.List(ctx, list, client.InNamespace(opts.Namespace)); err != nil {
		return err
	}
	start := 0
	if opts.Continue != "" {
		parsed, err := strconv.Atoi(opts.Continue)
		if err != nil {
			return err
		}
		start = parsed
	}
	end := min(start+int(opts.Limit), len(profiles.Items))
	if start < 0 || start > end {
		return errors.New("invalid page")
	}
	if end < len(profiles.Items) {
		profiles.Continue = strconv.Itoa(end)
	}
	profiles.Items = profiles.Items[start:end]
	return nil
}

func TestProfileDiscoveryPagesAuthoritativeCRDSource(t *testing.T) {
	g, writer, profile := profileGateway(t, testStore(t))
	for _, name := range []string{"second", "third"} {
		copy := profile.DeepCopy()
		copy.Name, copy.UID, copy.ResourceVersion = name, types.UID(name), ""
		if err := writer.Client.Create(t.Context(), copy); err != nil {
			t.Fatal(err)
		}
	}
	g.profiles.reader = &profilePageReader{Reader: writer}
	var page struct {
		Subject string           `json:"caller_subject"`
		Source  string           `json:"source"`
		Items   []ProfileSummary `json:"profiles"`
		More    bool             `json:"more"`
		Next    string           `json:"next_cursor"`
	}
	response := conversationRequest(g, http.MethodGet, "/v1/namespaces/team/profiles?limit=2", "", "")
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &page) != nil || len(page.Items) != 2 ||
		page.Subject != _testConversationSubject || page.Source != "crd" || !page.More || page.Next == "" {
		t.Fatal("invalid Profile discovery page", response.Body.String())
	}
	response = conversationRequest(g, http.MethodGet, "/v1/namespaces/team/profiles?limit=2&after="+page.Next, "", "")
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &page) != nil || len(page.Items) != 1 || page.More || page.Next != "" {
		t.Fatal("Profile cursor did not reach final page", response.Body.String())
	}
	for _, path := range []string{"/v1/namespaces/team/profiles/payments", "/v1/namespaces/team/catalog"} {
		if response := conversationRequest(g, http.MethodGet, path, "", ""); response.Code != http.StatusOK {
			t.Fatal(path, response.Code)
		}
	}
	if response := conversationRequest(g, http.MethodGet, "/v1/namespaces/other/profiles/sandbox", "", ""); response.Code != http.StatusNotFound {
		t.Fatal("Profile discovery crossed scope namespace")
	}
	for _, query := range []string{"limit=0", "limit=101", "limit=1&limit=2", "after="} {
		if response := conversationRequest(g, http.MethodGet, "/v1/namespaces/team/profiles?"+query, "", ""); response.Code != http.StatusBadRequest {
			t.Fatal("invalid Profile page accepted", query, response.Code)
		}
	}
}

func TestProfileCatalogReflectsLivePolicyAndReadiness(t *testing.T) {
	g, writer, profile := profileGateway(t, testStore(t))
	read := func() ProfileSummary {
		t.Helper()
		response := conversationRequest(g, http.MethodGet, "/v1/namespaces/team/catalog", "", "")
		var catalog struct {
			Source string           `json:"profiles_source"`
			Items  []ProfileSummary `json:"profiles"`
		}
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &catalog) != nil ||
			catalog.Source != _profileSourceCRD || len(catalog.Items) != 1 {
			t.Fatal("catalog omitted current CR Profiles", response.Code, response.Body.String())
		}
		return catalog.Items[0]
	}
	if !read().Ready {
		t.Fatal("current Profile absent from catalog readiness")
	}
	var current agentsv1alpha1.Profile
	if err := writer.Get(t.Context(), client.ObjectKeyFromObject(profile), &current); err != nil {
		t.Fatal(err)
	}
	current.Generation++
	limit := int64(75)
	current.Spec.SpendControl = agentsv1alpha1.SessionSpendControl{Mode: _scenarioLimited, LimitCents: &limit}
	if err := writer.Update(t.Context(), &current); err != nil {
		t.Fatal(err)
	}
	observed := read()
	if observed.Ready || observed.Reason == "" || observed.SpendControl.Mode != _scenarioLimited {
		t.Fatal("catalog hid live Profile change or used stale readiness")
	}
	current.Status.ObservedGeneration = current.Generation
	current.Status.Conditions = profileReadyConditions(current.Generation)
	current.Status.PolicyDigest = agentsv1alpha1.ProfilePolicyDigest(current.Spec)
	if err := writer.Update(t.Context(), &current); err != nil {
		t.Fatal(err)
	}
	if observed := read(); !observed.Ready || *observed.SpendControl.LimitCents != limit {
		t.Fatal("catalog required a gateway restart to observe readiness")
	}
	if err := writer.Delete(t.Context(), &current); err != nil {
		t.Fatal(err)
	}
	response := conversationRequest(g, http.MethodGet, "/v1/namespaces/team/catalog", "", "")
	var catalog struct {
		Items []ProfileSummary `json:"profiles"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &catalog) != nil || len(catalog.Items) != 0 {
		t.Fatal("catalog retained retired Profile policy")
	}
}

func TestProfileConcurrentAcceptanceAcrossRevisionsKeepsOneSnapshot(t *testing.T) {
	store := testStore(t)
	first, writer, _ := profileGateway(t, store)
	second, err := New(store, writer, simulatorForTest(), "writer", nil)
	if err != nil || second.EnableProfiles(writer) != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := range 12 {
		group.Go(func() {
			g := first
			if i%2 == 1 {
				g = second
			}
			if response := conversationRequest(g, http.MethodPost, _conversationPath, "same-profile-key", _profileTestBody); response.Code != http.StatusAccepted {
				t.Error(response.Code)
			}
		})
	}
	group.Wait()
	record := readWorkRecord(t, store, conversationRecordID(_testNamespace, _testConversationSubject, "same-profile-key"))
	if len(record.Work.Attempts) != 1 || len(record.Work.Events) != 1 || record.Version != _recordVersion {
		t.Fatal("concurrent Profile acceptance duplicated policy or input")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := gCreateCanceled(first, ctx); err == nil {
		t.Fatal("canceled Profile lookup admitted fresh input")
	}
	if _, err := store.Get(t.Context(), conversationRecordID(_testNamespace, _testConversationSubject, "canceled-profile")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("canceled admission left a partial record", err)
	}
}

func gCreateCanceled(g *Gateway, ctx context.Context) (Record, WorkEvent, error) {
	return g.createConversation(ctx, requestScope{Namespace: _testNamespace, Subject: _testConversationSubject}, "canceled-profile",
		ConversationInput{RouteID: _profileTestName, Text: "Canceled"})
}
