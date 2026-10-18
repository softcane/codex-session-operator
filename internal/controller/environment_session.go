package controller

import (
	"context"
	"errors"
	"path"
	"reflect"
	"slices"
	"strings"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// EnvironmentSessionProvider creates either mode with a resolved in-memory request.
type EnvironmentSessionProvider interface {
	GetHostedRuntime(context.Context, string) (openaiapi.HostedRuntime, error)
	GetTemplate(context.Context, string) (openaiapi.Template, error)
}

var _ EnvironmentSessionProvider = (*openaiapi.Client)(nil)

func (r *SessionReconciler) resolveEnvironment(ctx context.Context, s *agentsv1alpha1.Session) (*agentsv1alpha1.SessionEnvironmentSnapshot, map[string]any, string, string, error) {
	if s.Spec.EnvironmentRef == nil {
		return nil, nil, invalidConfigurationReason, "Session environmentRef is required", nil
	}
	if r.Environments == nil || r.AgentOwnerScope == "" {
		return nil, nil, "EnvironmentProviderRequired", "Configure the Environment provider and installation/project scope", nil
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	var environment agentsv1alpha1.Environment
	if err := reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Spec.EnvironmentRef.Name}, &environment); err != nil {
		return nil, nil, environmentNotReadyReason, "Referenced Environment could not be read", client.IgnoreNotFound(err)
	}
	ready := meta.FindStatusCondition(environment.Status.Conditions, readyCondition)
	if !environment.DeletionTimestamp.IsZero() || ready == nil || ready.Status != metav1.ConditionTrue || ready.ObservedGeneration != environment.Generation || environment.Status.OwnerScope != r.AgentOwnerScope {
		return nil, nil, environmentNotReadyReason, "Referenced Environment must be current, Ready, owned and not deleting", nil
	}
	profile := environment.Spec.DeepCopy()
	snapshot := &agentsv1alpha1.SessionEnvironmentSnapshot{Name: environment.Name, UID: string(environment.UID), Generation: environment.Generation,
		OpenAITemplateID: environment.Status.OpenAITemplateID, ConfigurationDigest: environment.Status.ConfigurationDigest, Profile: *profile}
	if profile.Mode == "selfHosted" {
		fields, reason, message, err := r.resolveSelfHostedEnvironment(ctx, s, reader, &environment, profile, snapshot)
		if reason != "" || err != nil {
			return nil, nil, reason, message, err
		}
		return snapshot, fields, "", "", nil
	}
	if profile.Mode == _noneMode && r.Gateway != nil {
		if openaiapi.EnvironmentDigest(*profile, nil) != environment.Status.ConfigurationDigest {
			return nil, nil, environmentNotReadyReason, "None profile digest is stale", nil
		}
		return snapshot, map[string]any{_wireType: _noneMode}, "", "", nil
	}
	if profile.Mode != openaiHostedMode || snapshot.OpenAITemplateID == "" {
		return nil, nil, "GatewayRequired", "Environment mode cannot create a Session in this phase", nil
	}
	secret, source, err := readEnvironmentSource(ctx, reader, s.Namespace, profile.OpenAIHosted)
	if err != nil {
		return nil, nil, "ProtectedSourceUnavailable", "Could not resolve protected Environment source", err
	}
	if openaiapi.EnvironmentDigest(*profile, source) != environment.Status.ConfigurationDigest || !reflect.DeepEqual(source, environment.Status.SourceVersion) {
		return nil, nil, environmentNotReadyReason, "Protected source versions have changed; wait for Environment reconciliation", nil
	}
	fields, err := openaiapi.HostedConfiguration(profile.OpenAIHosted, secret, openaiapi.ManagedTemplateName(profile.OpenAIHosted.Name, string(environment.UID), environment.Status.OwnerScope))
	if err != nil {
		return nil, nil, invalidConfigurationReason, err.Error(), nil
	}
	remote, err := r.Environments.GetTemplate(ctx, snapshot.OpenAITemplateID)
	if err != nil {
		return nil, nil, environmentNotReadyReason, "Could not observe hosted template", err
	}
	if remote.ID != snapshot.OpenAITemplateID || !ownedTemplate(&environment, remote) || !openaiapi.TemplateMatches(fields, remote) {
		return nil, nil, environmentNotReadyReason, "Hosted template identity or observable configuration has drifted", nil
	}
	delete(fields, "name")
	fields[_wireType] = "openai_hosted"
	fields["environment_template_id"] = snapshot.OpenAITemplateID
	fields["container_size"] = profile.OpenAIHosted.ContainerSize
	snapshot.SourceVersion = source
	snapshot.ObservableDigest = openaiapi.ObservableEnvironmentDigest(fields)
	return snapshot, fields, "", "", nil
}

func (r *SessionReconciler) resolveSelfHostedEnvironment(ctx context.Context, s *agentsv1alpha1.Session,
	reader client.Reader, environment *agentsv1alpha1.Environment, profile *agentsv1alpha1.EnvironmentSpec,
	snapshot *agentsv1alpha1.SessionEnvironmentSnapshot) (map[string]any, string, string, error) {
	if err := sandbox.ValidateProfile(profile.SelfHosted, r.ExecutorImages, r.CapabilityImages, r.WorkspaceStorageClasses); err != nil {
		return nil, invalidConfigurationReason, err.Error(), nil
	}
	if err := sandbox.ValidateRepositoryApproval(profile.SelfHosted.Repository, r.RepositoryURLs); err != nil {
		return nil, invalidConfigurationReason, err.Error(), nil
	}
	source, err := readRepositorySource(ctx, reader, s.Namespace, profile.SelfHosted.Repository)
	if err != nil {
		return nil, "ProtectedSourceUnavailable", "Could not resolve protected repository source", err
	}
	if openaiapi.EnvironmentDigest(*profile, source) != environment.Status.ConfigurationDigest || !reflect.DeepEqual(source, environment.Status.SourceVersion) {
		return nil, environmentNotReadyReason, "Local profile digest is stale", nil
	}
	snapshot.SourceVersion = source
	paths := make([]string, 0, len(profile.SelfHosted.Capabilities))
	for _, capability := range profile.SelfHosted.Capabilities {
		paths = append(paths, capability.Path)
	}
	return map[string]any{_wireType: "self_hosted", "workspace_directory": profile.SelfHosted.WorkspaceDirectory, "capability_directories": paths}, "", "", nil
}

func (r *SessionReconciler) trackHosted(ctx context.Context, s *agentsv1alpha1.Session, remote openaiapi.Session, base *agentsv1alpha1.Session) (ctrl.Result, error) {
	wait := ctrl.Result{RequeueAfter: observationInterval}
	if message := hostedSessionConfigurationProblem(s, remote); message != "" {
		return r.rejectRemoteSession(ctx, s, base, configurationMismatchReason, message)
	}
	s.Status.EnvironmentID = remote.EnvironmentID
	paths := append([]string{}, remote.CapabilityDirectories...)
	slices.Sort(paths)
	if len(s.Status.ResolvedCapabilityDirectories) == 0 {
		s.Status.ResolvedCapabilityDirectories = paths
	}
	if r.Environments == nil {
		return r.problem(ctx, s, "EnvironmentProviderRequired", "Configure hosted runtime observation", nil)
	}
	runtime, err := r.Environments.GetHostedRuntime(ctx, remote.EnvironmentID)
	if err != nil {
		setState(s, "Connecting", metav1.ConditionTrue, "EnvironmentObservationFailed", "Could not observe hosted runtime setup")
		return wait, errors.Join(err, r.patchStatus(ctx, s, base))
	}
	setState(s, "Connecting", metav1.ConditionTrue, "HostedProvisioning", "OpenAI-hosted setup is pending")
	if runtime.ID != remote.EnvironmentID {
		setState(s, "Failed", metav1.ConditionFalse, configurationMismatchReason, "Observed runtime identity does not match")
	} else if runtime.Status == remoteFailedStatus || remote.Status == remoteFailedStatus {
		setState(s, "Failed", metav1.ConditionFalse, "HostedSetupFailed", "OpenAI reports hosted setup failure; provider details are withheld")
	} else if runtime.Status == "connected" {
		s.Status.Phase = "Ready"
		for _, kind := range []string{readyCondition, connectedCondition} {
			meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{Type: kind, Status: metav1.ConditionTrue, Reason: "HostedEnvironmentConnected", Message: "OpenAI reports hosted setup complete and environment connected", ObservedGeneration: s.Generation})
		}
	} else {
		wait.RequeueAfter = 5 * time.Second
	}
	return wait, r.patchStatus(ctx, s, base)
}

// hostedSessionConfigurationProblem checks only frozen, observable configuration;
// runtime provisioning observations remain separate from supported settings edits.
func hostedSessionConfigurationProblem(s *agentsv1alpha1.Session, remote openaiapi.Session) string {
	if s.Status.EnvironmentID != "" && s.Status.EnvironmentID != remote.EnvironmentID {
		return "Hosted runtime identity changed after creation"
	}
	if remote.SelfHosted || remote.EnvironmentType != "openai_hosted" || remote.EnvironmentID == "" || remote.EnvironmentConfigurationDigest != s.Status.EnvironmentSnapshot.ObservableDigest {
		return "Remote hosted configuration differs from the frozen creation profile"
	}
	if !hostedCapabilityPathsMatch(s.Status.EnvironmentSnapshot.Profile.OpenAIHosted, remote.CapabilityDirectories) {
		return "Hosted capability paths do not match declared and installed capabilities"
	}
	if len(s.Status.ResolvedCapabilityDirectories) > 0 {
		paths := slices.Clone(remote.CapabilityDirectories)
		slices.Sort(paths)
		if !slices.Equal(s.Status.ResolvedCapabilityDirectories, paths) {
			return "Hosted capability installation paths changed after resolution"
		}
	}
	return ""
}

func (r *SessionReconciler) environmentSessionRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	var sessions agentsv1alpha1.SessionList
	if err := r.List(ctx, &sessions, client.InNamespace(obj.GetNamespace()), client.MatchingFields{environmentReferenceIndex: obj.GetName()}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(sessions.Items))
	for i := range sessions.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&sessions.Items[i])})
	}
	return requests
}

func sessionEnvironmentMatches(s *agentsv1alpha1.Session, remote openaiapi.Session) bool {
	if s.Status.EnvironmentSnapshot == nil {
		return true
	}
	profile := s.Status.EnvironmentSnapshot.Profile.SelfHosted
	if profile == nil {
		return false
	}
	paths := make([]string, 0, len(profile.Capabilities))
	for _, capability := range profile.Capabilities {
		paths = append(paths, capability.Path)
	}
	return reflect.DeepEqual(paths, append([]string{}, remote.CapabilityDirectories...))
}

func sessionAgentBlock(remote openaiapi.Session, environment *agentsv1alpha1.SessionEnvironmentSnapshot) (string, string) {
	if remote.AgentConfiguration == nil {
		return remote.AgentBlockReason, remote.AgentBlockMessage
	}
	return agentEnvironmentBlock(remote.AgentConfiguration, environment)
}
func agentEnvironmentBlock(fields map[string]any, environment *agentsv1alpha1.SessionEnvironmentSnapshot) (string, string) {
	var profile *agentsv1alpha1.EnvironmentSpec
	if environment != nil {
		profile = &environment.Profile
	}
	return openaiapi.AgentEnvironmentBlock(fields, profile)
}

// hostedCapabilityPathsMatch separates requested paths from API-generated installation roots.
func hostedCapabilityPathsMatch(profile *agentsv1alpha1.HostedEnvironment, paths []string) bool {
	if profile == nil {
		return false
	}
	requested := make(map[string]bool, len(profile.CapabilityDirectories))
	for _, directory := range profile.CapabilityDirectories {
		requested[directory] = false
	}
	plugins := make(map[string]bool, len(profile.Plugins))
	for _, plugin := range profile.Plugins {
		plugins[plugin.Name] = false
	}
	inlineSkills := make(map[string]bool)
	references := 0
	for _, skill := range profile.Skills {
		if skill.Inline != nil {
			inlineSkills[skill.Inline.Name] = false
		} else {
			references++
		}
	}
	seen := make(map[string]bool, len(paths))
	for _, directory := range paths {
		if seen[directory] || path.Clean(directory) != directory {
			return false
		}
		seen[directory] = true
		if _, ok := requested[directory]; ok {
			requested[directory] = true
			continue
		}
		kind := "plugins"
		prefix := "/workspace/.managed-agents/" + kind + "/"
		if strings.HasPrefix(directory, prefix) {
			if !matchGeneratedPath(directory, prefix, plugins) {
				return false
			}
			continue
		}
		prefix = "/workspace/.managed-agents/skills/"
		if !strings.HasPrefix(directory, prefix) || strings.Contains(strings.TrimPrefix(directory, prefix), "/") {
			return false
		}
		if !matchGeneratedPath(directory, prefix, inlineSkills) {
			if references == 0 {
				return false
			}
			references--
		}
	}
	return references == 0 && allPathsPresent(requested) && allPathsPresent(plugins) && allPathsPresent(inlineSkills)
}

func allPathsPresent(expected map[string]bool) bool {
	for _, present := range expected {
		if !present {
			return false
		}
	}
	return true
}

func matchGeneratedPath(directory, prefix string, expected map[string]bool) bool {
	for name, present := range expected {
		if !present && generatedCapabilityPath(directory, prefix, name) {
			expected[name] = true
			return true
		}
	}
	return false
}

func generatedCapabilityPath(directory, prefix, name string) bool {
	value := strings.TrimPrefix(directory, prefix+name+"-")
	return value != directory && len(value) >= 8 && len(value) <= 64 && strings.Trim(value, "0123456789abcdef") == ""
}
