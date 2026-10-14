package gateway

import (
	"context"
	"embed"
	"errors"
	"net/http"
	"slices"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//go:embed ui/index.html ui/app.js ui/style.css
var uiFiles embed.FS

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

func (g *Gateway) serveUI(w http.ResponseWriter, r *http.Request, parts []string) bool {
	if len(parts) == 0 || parts[0] != "ui" {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodError(w)
		return true
	}
	path := "ui/index.html"
	contentType := "text/html; charset=utf-8"
	if len(parts) == 2 {
		switch parts[1] {
		case "app.js":
			path, contentType = "ui/app.js", "text/javascript; charset=utf-8"
		case "style.css":
			path, contentType = "ui/style.css", "text/css; charset=utf-8"
		default:
			http.NotFound(w, r)
			return true
		}
	} else if len(parts) != 1 {
		http.NotFound(w, r)
		return true
	}
	data, err := uiFiles.ReadFile(path)
	if err != nil {
		http.Error(w, "Interface unavailable", http.StatusServiceUnavailable)
		return true
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	_, _ = w.Write(data)
	return true
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

type workRouteSummary struct {
	ID           string                              `json:"id"`
	Source       string                              `json:"source"`
	Agent        string                              `json:"agent"`
	Environment  string                              `json:"environment"`
	Vaults       []string                            `json:"vaults,omitempty"`
	SpendControl *agentsv1alpha1.SessionSpendControl `json:"spend_control"`
	Ready        bool                                `json:"ready"`
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
		writeJSON(w, http.StatusOK, map[string]any{_sessionsSegment: items, "more": sessions.Continue != "" || len(sessions.Items) > limit})
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
	var agents agentsv1alpha1.AgentList
	var environments agentsv1alpha1.EnvironmentList
	var vaults agentsv1alpha1.VaultList
	for _, object := range []client.ObjectList{&agents, &environments, &vaults} {
		if err := g.reader.List(r.Context(), object, client.InNamespace(principal.Namespace), client.Limit(101)); err != nil {
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
	routeItems, routesMore, err := g.catalogWorkRoutes(r.Context(), principal.Namespace)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"caller_subject": principal.Subject,
		"agents": agentItems, "environments": environmentItems, "vaults": vaultItems,
		"work_routes": routeItems,
		"more":        routesMore || agents.Continue != "" || environments.Continue != "" || vaults.Continue != "" || len(agentItems) > 100 || len(environmentItems) > 100 || len(vaultItems) > 100})
}

func (g *Gateway) catalogWorkRoutes(ctx context.Context, namespace string) ([]workRouteSummary, bool, error) {
	ids := make([]string, 0, len(g.workRoutes))
	for id, route := range g.workRoutes {
		if route.Namespace == namespace {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	limit := min(len(ids), 100)
	items := make([]workRouteSummary, 0, limit)
	for _, id := range ids[:limit] {
		route := g.workRoutes[id]
		ready, err := g.catalogWorkRouteReady(ctx, route)
		if err != nil {
			return nil, false, err
		}
		items = append(items, workRouteSummary{ID: id, Source: route.Source, Agent: route.Agent,
			Environment: route.Environment, Vaults: slices.Clone(route.Vaults),
			SpendControl: route.SpendControl.DeepCopy(), Ready: ready})
	}
	return items, len(ids) > limit, nil
}

func (g *Gateway) catalogWorkRouteReady(ctx context.Context, route WorkRoute) (bool, error) {
	var agent agentsv1alpha1.Agent
	var environment agentsv1alpha1.Environment
	objects := make([]client.Object, 0, 2+len(route.Vaults))
	objects = append(objects, &agent, &environment)
	names := make([]string, 0, 2+len(route.Vaults))
	names = append(names, route.Agent, route.Environment)
	for _, name := range route.Vaults {
		objects = append(objects, &agentsv1alpha1.Vault{})
		names = append(names, name)
	}
	for i, object := range objects {
		if err := g.reader.Get(ctx, client.ObjectKey{Namespace: route.Namespace, Name: names[i]}, object); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		var conditions []metav1.Condition
		switch selected := object.(type) {
		case *agentsv1alpha1.Agent:
			conditions = selected.Status.Conditions
		case *agentsv1alpha1.Environment:
			conditions = selected.Status.Conditions
		case *agentsv1alpha1.Vault:
			conditions = selected.Status.Conditions
		}
		if !catalogReady(object, conditions) {
			return false, nil
		}
	}
	return true, nil
}

func catalogReady(object client.Object, conditions []metav1.Condition) bool {
	ready := meta.FindStatusCondition(conditions, _readyCondition)
	return object.GetDeletionTimestamp().IsZero() && ready != nil &&
		ready.Status == metav1.ConditionTrue && ready.ObservedGeneration == object.GetGeneration()
}
