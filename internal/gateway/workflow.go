package gateway

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strconv"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type WorkflowSummary struct {
	Name       string                                `json:"name"`
	UID        string                                `json:"uid"`
	Generation int64                                 `json:"generation"`
	Digest     string                                `json:"digest"`
	Spec       agentsv1alpha1.WorkflowTemplateSpec   `json:"spec"`
	Status     agentsv1alpha1.WorkflowTemplateStatus `json:"status"`
	Ready      bool                                  `json:"ready"`
	Reason     string                                `json:"reason"`
	Profiles   []ProfileSummary                      `json:"profiles"`
}

// workflow validates configuration only. Optional roles bind their live Profile on assignment.
func (g *Gateway) workflow(ctx context.Context, namespace, name string) (agentsv1alpha1.WorkflowTemplate, error) {
	var template agentsv1alpha1.WorkflowTemplate
	if err := g.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &template); err != nil {
		if apierrors.IsNotFound(err) {
			return template, os.ErrNotExist
		}
		return template, err
	}
	if template.UID == "" || template.Generation < 1 || !template.DeletionTimestamp.IsZero() {
		return template, errors.New("workflow unavailable")
	}
	if err := agentsv1alpha1.ValidateWorkflowTemplateSpec(template.Spec); err != nil {
		return template, err
	}
	return template, nil
}

func (g *Gateway) summarizeWorkflow(ctx context.Context, template agentsv1alpha1.WorkflowTemplate) WorkflowSummary {
	summary := WorkflowSummary{Name: template.Name, UID: string(template.UID), Generation: template.Generation, Digest: agentsv1alpha1.WorkflowTemplateSpecDigest(template.Spec), Spec: template.Spec, Status: template.Status, Profiles: make([]ProfileSummary, 0), Reason: "WorkflowTemplateNotCurrent"}
	condition := meta.FindStatusCondition(template.Status.Conditions, _readyCondition)
	summary.Ready = template.Generation > 0 && template.UID != "" && template.DeletionTimestamp.IsZero() && template.Status.ObservedGeneration == template.Generation && template.Status.SpecDigest == summary.Digest && condition != nil && condition.Status == metav1.ConditionTrue && condition.ObservedGeneration == template.Generation
	if summary.Ready {
		summary.Reason = "WorkflowTemplateReady"
	}
	if agentsv1alpha1.ValidateWorkflowTemplateSpec(template.Spec) != nil {
		summary.Ready = false
		summary.Reason = "InvalidConfiguration"
	}
	names := make([]string, 0, 1+len(template.Spec.Roles))
	names = append(names, template.Spec.Coordinator.ProfileRef.Name)
	seen := make(map[string]bool)
	for _, role := range template.Spec.Roles {
		names = append(names, role.ProfileRef.Name)
	}
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		profile, err := g.profileSummary(ctx, template.Namespace, name)
		if err != nil {
			profile = ProfileSummary{Name: name, ID: name, Namespace: template.Namespace, Reason: _profileObservationUnavailable}
		}
		summary.Profiles = append(summary.Profiles, profile)
		if !profile.Ready {
			summary.Ready = false
			summary.Reason = "ProfileNotReady"
		}
	}
	return summary
}

func (g *Gateway) serveWorkflows(w http.ResponseWriter, r *http.Request, parts []string, scope requestScope) bool {
	if len(parts) < 4 || parts[3] != "workflows" {
		return false
	}
	if r.Method != http.MethodGet {
		methodError(w)
		return true
	}
	ctx, cancel := context.WithTimeout(r.Context(), _profileTimeout)
	defer cancel()
	if len(parts) == 5 {
		var template agentsv1alpha1.WorkflowTemplate
		if err := g.reader.Get(ctx, client.ObjectKey{Namespace: scope.Namespace, Name: parts[4]}, &template); err != nil {
			if apierrors.IsNotFound(err) {
				err = os.ErrNotExist
			}
			writeConversationError(w, err)
			return true
		}
		writeJSON(w, http.StatusOK, g.summarizeWorkflow(ctx, template))
		return true
	}
	if len(parts) != 4 {
		http.NotFound(w, r)
		return true
	}
	limit, after := 100, r.URL.Query().Get("after")
	if values, ok := r.URL.Query()["limit"]; ok {
		parsed, err := strconv.Atoi(values[0])
		if len(values) != 1 || err != nil || parsed < 1 || parsed > 100 {
			http.Error(w, "Invalid Workflow page limit", http.StatusBadRequest)
			return true
		}
		limit = parsed
	}
	if values, ok := r.URL.Query()["after"]; ok && (len(values) != 1 || after == "" || len(after) > 4096) {
		http.Error(w, "Invalid Workflow page cursor", http.StatusBadRequest)
		return true
	}
	var list agentsv1alpha1.WorkflowTemplateList
	if err := g.reader.List(ctx, &list, client.InNamespace(scope.Namespace), client.Limit(limit), client.Continue(after)); err != nil {
		writeConversationError(w, errUnavailable)
		return true
	}
	summaries := make([]WorkflowSummary, 0, len(list.Items))
	for _, template := range list.Items {
		if template.Namespace != scope.Namespace {
			writeConversationError(w, errUnavailable)
			return true
		}
		summaries = append(summaries, g.summarizeWorkflow(ctx, template))
	}
	writeJSON(w, http.StatusOK, map[string]any{"workflows": summaries, "more": list.Continue != "", _nextCursorField: list.Continue})
	return true
}
