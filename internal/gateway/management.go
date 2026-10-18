package gateway

import (
	"context"
	"errors"
	"net/http"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const _readyCondition = "Ready"

// EnableSessionManagement is called once during startup with the Kubernetes
// client used by the gateway service. Requests are scoped to the authenticated
// principal's namespace because the gateway service account spans namespaces.
func (g *Gateway) EnableSessionManagement(writer client.Client) error {
	if writer == nil {
		return errors.New("session management requires a Kubernetes writer")
	}
	g.sessionWriter = writer
	return nil
}

type sessionSummary struct {
	Name         string                              `json:"name"`
	UID          string                              `json:"uid"`
	Agent        string                              `json:"agent"`
	Environment  string                              `json:"environment"`
	Phase        string                              `json:"phase"`
	Ready        bool                                `json:"ready"`
	SpendControl *agentsv1alpha1.SessionSpendControl `json:"spend_control,omitempty"`
}

func summarizeSession(session *agentsv1alpha1.Session) sessionSummary {
	result := sessionSummary{Name: session.Name, UID: string(session.UID), Phase: session.Status.Phase,
		Ready: meta.IsStatusConditionTrue(session.Status.Conditions, _readyCondition), SpendControl: session.Spec.SpendControl}
	if session.Spec.AgentRef != nil {
		result.Agent = session.Spec.AgentRef.Name
	}
	if session.Spec.EnvironmentRef != nil {
		result.Environment = session.Spec.EnvironmentRef.Name
	}
	return result
}

func (g *Gateway) handleSessionCollection(w http.ResponseWriter, r *http.Request, principal Principal) {
	if g.sessionWriter == nil {
		http.Error(w, "Session management unavailable", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		var sessions agentsv1alpha1.SessionList
		if err := g.reader.List(r.Context(), &sessions, client.InNamespace(principal.Namespace), client.Limit(101)); err != nil {
			writeError(w, err)
			return
		}
		limit := min(len(sessions.Items), 100)
		items := make([]sessionSummary, 0, limit)
		for i := range limit {
			items = append(items, summarizeSession(&sessions.Items[i]))
		}
		writeJSON(w, http.StatusOK, map[string]any{_sessionsSegment: items, _moreField: sessions.Continue != "" || len(sessions.Items) > limit})
	case http.MethodPost:
		var input struct {
			Name string                     `json:"name"`
			Spec agentsv1alpha1.SessionSpec `json:"spec"`
		}
		if decode(w, r, &input) != nil || len(validation.IsDNS1123Subdomain(input.Name)) != 0 ||
			input.Spec.AgentRef == nil || input.Spec.AgentRef.Name == "" || input.Spec.EnvironmentRef == nil || input.Spec.EnvironmentRef.Name == "" || input.Spec.SpendControl == nil {
			http.Error(w, "Name, Agent, Environment, and spend choice are required", http.StatusBadRequest)
			return
		}
		if _, _, err := openaiapi.SessionSpendControlField(input.Spec.SpendControl); err != nil {
			http.Error(w, "Invalid spend choice", http.StatusBadRequest)
			return
		}
		session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Name: input.Name, Namespace: principal.Namespace}, Spec: input.Spec}
		if err := g.sessionWriter.Create(r.Context(), session); err != nil {
			if apierrors.IsAlreadyExists(err) {
				http.Error(w, "Session name already exists; inspect the existing Session before retrying", http.StatusConflict)
				return
			}
			if apierrors.IsInvalid(err) {
				http.Error(w, "Session configuration was rejected", http.StatusBadRequest)
				return
			}
			writeError(w, err)
			return
		}
		w.Header().Set("Location", "/v1/namespaces/"+principal.Namespace+"/sessions/"+input.Name)
		writeJSON(w, http.StatusCreated, summarizeSession(session))
	default:
		methodError(w)
	}
}

func (g *Gateway) handleCatalog(w http.ResponseWriter, r *http.Request, principal Principal) {
	if r.Method != http.MethodGet {
		methodError(w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), _profileTimeout)
	defer cancel()
	var agents agentsv1alpha1.AgentList
	var environments agentsv1alpha1.EnvironmentList
	var vaults agentsv1alpha1.VaultList
	for _, object := range []client.ObjectList{&agents, &environments, &vaults} {
		if err := g.reader.List(ctx, object, client.InNamespace(principal.Namespace), client.Limit(101)); err != nil {
			writeError(w, err)
			return
		}
	}
	type catalogItem struct {
		Name  string `json:"name"`
		Ready bool   `json:"ready"`
		Mode  string `json:"mode,omitempty"`
	}
	agentItems := make([]catalogItem, 0, len(agents.Items))
	for i := range agents.Items {
		a := &agents.Items[i]
		agentItems = append(agentItems, catalogItem{Name: a.Name, Ready: catalogReady(a, a.Status.Conditions)})
	}
	environmentItems := make([]catalogItem, 0, len(environments.Items))
	for i := range environments.Items {
		e := &environments.Items[i]
		environmentItems = append(environmentItems, catalogItem{Name: e.Name, Ready: catalogReady(e, e.Status.Conditions), Mode: e.Spec.Mode})
	}
	vaultItems := make([]catalogItem, 0, len(vaults.Items))
	for i := range vaults.Items {
		v := &vaults.Items[i]
		vaultItems = append(vaultItems, catalogItem{Name: v.Name, Ready: catalogReady(v, v.Status.Conditions)})
	}
	profiles, nextProfile, err := g.profileSummaries(ctx, principal.Namespace, "", 100)
	if err != nil {
		writeConversationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{_callerSubjectField: principal.Subject,
		"profiles_source": g.profileSource(),
		"profiles":        profiles, "profiles_more": nextProfile != "", "profiles_next_cursor": nextProfile,
		"agents": agentItems, "environments": environmentItems, "vaults": vaultItems,
		_moreField: agents.Continue != "" || environments.Continue != "" || vaults.Continue != "" || len(agentItems) > 100 || len(environmentItems) > 100 || len(vaultItems) > 100})
}

func catalogReady(object client.Object, conditions []metav1.Condition) bool {
	ready := meta.FindStatusCondition(conditions, _readyCondition)
	return object.GetDeletionTimestamp().IsZero() && ready != nil &&
		ready.Status == metav1.ConditionTrue && ready.ObservedGeneration == object.GetGeneration()
}
