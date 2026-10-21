package gateway

import (
	"context"
	"errors"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strconv"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	_profilesSegment                  = "profiles"
	_profileTimeout                   = 3 * time.Second
	_profileSourceCRD                 = "crd"
	_profileSourceName                = "profile"
	_callerSubjectField               = "caller_subject"
	_sourceField                      = "source"
	_dependencyIdentityMismatchReason = "DependencyIdentityMismatch"
	_selfHostedMode                   = "SelfHosted"
	_selfHostedProviderType           = "self_hosted"
	_connectedStatus                  = "connected"
	_identitySegment                  = "identity"
	_workspacePreparingReason         = "WorkspacePreparing"
	_remoteSessionMissingReason       = "RemoteSessionMissing"
	_errorField                       = "error"
	_profileObservationUnavailable    = "ProfileObservationUnavailable"
)

// ProfileProvenance is safe, immutable source evidence for an accepted attempt.
// Generation is observed provenance; UID is the dependency replacement fence.
type ProfileProvenance struct {
	Namespace       string                                   `json:"namespace"`
	Name            string                                   `json:"name"`
	UID             string                                   `json:"uid"`
	Generation      int64                                    `json:"generation"`
	ResourceVersion string                                   `json:"resource_version"`
	PolicyDigest    string                                   `json:"policy_digest"`
	Agent           agentsv1alpha1.ProfileDependencyStatus   `json:"agent"`
	Environment     agentsv1alpha1.ProfileDependencyStatus   `json:"environment"`
	Vaults          []agentsv1alpha1.ProfileDependencyStatus `json:"vaults,omitempty"`
}

type profileSelection struct {
	route      profileRoute
	provenance *ProfileProvenance
}

// ProfileResolver reads authoritative Kubernetes objects on each selection.
// It has no mutable route cache and is shared by HTTP and worker views.
type ProfileResolver struct {
	reader client.Reader
}

type profileUnavailableError struct{ reason string }

func (e *profileUnavailableError) Error() string { return "profile unavailable: " + e.reason }

// EnableProfiles selects authoritative CR profiles for conversations.
func (g *Gateway) EnableProfiles(writer client.Client) error {
	if writer == nil {
		return errors.New("profile Session writer required")
	}
	g.sessionWriter = writer
	g.profiles = &ProfileResolver{reader: g.reader}
	return nil
}

func (g *Gateway) profileSource() string { return _profileSourceCRD }

func (g *Gateway) selectProfile(ctx context.Context, namespace, name string) (profileSelection, error) {
	if g.profiles == nil {
		return profileSelection{}, &profileUnavailableError{reason: _profileObservationUnavailable}
	}
	return g.profiles.selectProfile(ctx, namespace, name)
}

func (p *ProfileResolver) selectProfile(ctx context.Context, namespace, name string) (profileSelection, error) {
	ctx, cancel := context.WithTimeout(ctx, _profileTimeout)
	defer cancel()
	if len(validation.IsDNS1123Subdomain(name)) != 0 {
		return profileSelection{}, os.ErrNotExist
	}
	var profile agentsv1alpha1.Profile
	if err := p.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &profile); err != nil {
		if apierrors.IsNotFound(err) {
			return profileSelection{}, os.ErrNotExist
		}
		return profileSelection{}, &profileUnavailableError{reason: "ObservationUnavailable"}
	}
	if profile.Namespace != namespace || profile.Name != name {
		return profileSelection{}, &profileUnavailableError{reason: "ProfileIdentityMismatch"}
	}
	return p.selection(ctx, &profile)
}

func (p *ProfileResolver) selection(ctx context.Context, profile *agentsv1alpha1.Profile) (profileSelection, error) {
	fail := func(reason string) (profileSelection, error) {
		return profileSelection{}, &profileUnavailableError{reason: reason}
	}
	if profile.UID == "" || profile.Generation < 1 || profile.ResourceVersion == "" || !profile.DeletionTimestamp.IsZero() {
		return fail("ProfileRetired")
	}
	if _, present, err := openaiapi.SessionSpendControlField(&profile.Spec.SpendControl); err != nil || !present ||
		len(profile.Spec.VaultRefs) > 16 {
		return fail("InvalidPolicy")
	}
	if !catalogReady(profile, profile.Status.Conditions) ||
		profile.Status.ObservedGeneration != profile.Generation ||
		profile.Status.PolicyDigest != agentsv1alpha1.ProfilePolicyDigest(profile.Spec) ||
		profile.Status.Agent == nil || profile.Status.Environment == nil ||
		len(profile.Status.Vaults) != len(profile.Spec.VaultRefs) {
		return fail("ProfileNotReady")
	}
	var agent agentsv1alpha1.Agent
	var environment agentsv1alpha1.Environment
	if err := p.dependency(ctx, profile.Namespace, profile.Spec.AgentRef.Name, *profile.Status.Agent, &agent); err != nil {
		return profileSelection{}, err
	}
	if err := p.dependency(ctx, profile.Namespace, profile.Spec.EnvironmentRef.Name, *profile.Status.Environment, &environment); err != nil {
		return profileSelection{}, err
	}
	if environment.Spec.Mode == _selfHostedMode {
		pool, err := sandbox.ResolvePool(ctx, p.reader, profile.Namespace, environment.Spec.SelfHosted)
		if err != nil {
			return fail("SandboxPoolUnavailable")
		}
		if !reflect.DeepEqual(pool, environment.Status.SandboxPool) ||
			openaiapi.SandboxEnvironmentDigest(environment.Spec, pool) != environment.Status.ConfigurationDigest {
			return fail("SandboxPoolChanged")
		}
	}
	seen := make(map[string]bool, len(profile.Spec.VaultRefs))
	vaults := make(map[string]agentsv1alpha1.ProfileDependencyStatus, len(profile.Status.Vaults))
	for _, dependency := range profile.Status.Vaults {
		if _, exists := vaults[dependency.Name]; exists {
			return fail("InvalidPolicy")
		}
		vaults[dependency.Name] = dependency
	}
	route := profileRoute{ID: profile.Name, Namespace: profile.Namespace, Source: _profileSourceName,
		Agent: profile.Spec.AgentRef.Name, Environment: profile.Spec.EnvironmentRef.Name,
		SpendControl: profile.Spec.SpendControl.DeepCopy()}
	for _, ref := range profile.Spec.VaultRefs {
		dependency, ok := vaults[ref.Name]
		if !ok || seen[ref.Name] {
			return fail("InvalidPolicy")
		}
		seen[ref.Name] = true
		var vault agentsv1alpha1.Vault
		if err := p.dependency(ctx, profile.Namespace, ref.Name, dependency, &vault); err != nil {
			return profileSelection{}, err
		}
		route.Vaults = append(route.Vaults, ref.Name)
	}
	provenance := &ProfileProvenance{Namespace: profile.Namespace, Name: profile.Name,
		UID: string(profile.UID), Generation: profile.Generation, ResourceVersion: profile.ResourceVersion,
		PolicyDigest: profile.Status.PolicyDigest, Agent: *profile.Status.Agent,
		Environment: *profile.Status.Environment, Vaults: slices.Clone(profile.Status.Vaults)}
	return profileSelection{route: route, provenance: provenance}, nil
}

func (p *ProfileResolver) dependency(ctx context.Context, namespace, name string,
	expected agentsv1alpha1.ProfileDependencyStatus, object client.Object) error {
	if expected.Name != name || expected.UID == "" || len(validation.IsDNS1123Subdomain(name)) != 0 {
		return &profileUnavailableError{reason: _dependencyIdentityMismatchReason}
	}
	if err := p.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, object); err != nil {
		if apierrors.IsNotFound(err) {
			return &profileUnavailableError{reason: "DependencyMissing"}
		}
		return &profileUnavailableError{reason: "ObservationUnavailable"}
	}
	if object.GetNamespace() != namespace || object.GetName() != name ||
		string(object.GetUID()) != expected.UID || object.GetGeneration() != expected.Generation ||
		!catalogReady(object, profileDependencyConditions(object)) {
		return &profileUnavailableError{reason: "DependencyNotReady"}
	}
	return nil
}

func profileDependencyConditions(object client.Object) []metav1.Condition {
	switch selected := object.(type) {
	case *agentsv1alpha1.Agent:
		return selected.Status.Conditions
	case *agentsv1alpha1.Environment:
		return selected.Status.Conditions
	case *agentsv1alpha1.Vault:
		return selected.Status.Conditions
	default:
		return nil
	}
}

// ProfileSummary exposes only safe selection metadata and readiness reasons.
type ProfileSummary struct {
	ID              string                              `json:"id"`
	Name            string                              `json:"name"`
	Namespace       string                              `json:"namespace"`
	UID             string                              `json:"uid,omitempty"`
	Generation      int64                               `json:"generation,omitempty"`
	ResourceVersion string                              `json:"resource_version,omitempty"`
	Agent           string                              `json:"agent"`
	Environment     string                              `json:"environment"`
	Vaults          []string                            `json:"vaults,omitempty"`
	SpendControl    *agentsv1alpha1.SessionSpendControl `json:"spend_control"`
	Ready           bool                                `json:"ready"`
	Reason          string                              `json:"reason,omitempty"`
}

func summarizeProfile(profile *agentsv1alpha1.Profile, err error) ProfileSummary {
	summary := ProfileSummary{ID: profile.Name, Name: profile.Name, Namespace: profile.Namespace,
		UID: string(profile.UID), Generation: profile.Generation, ResourceVersion: profile.ResourceVersion,
		Agent: profile.Spec.AgentRef.Name, Environment: profile.Spec.EnvironmentRef.Name,
		SpendControl: profile.Spec.SpendControl.DeepCopy(), Ready: err == nil}
	for _, ref := range profile.Spec.VaultRefs {
		summary.Vaults = append(summary.Vaults, ref.Name)
	}
	if unavailable, ok := errors.AsType[*profileUnavailableError](err); ok {
		summary.Reason = unavailable.reason
	}
	return summary
}

func (g *Gateway) handleProfiles(w http.ResponseWriter, r *http.Request, parts []string, scope requestScope) {
	if r.Method != http.MethodGet || len(parts) != 4 && len(parts) != 5 {
		methodError(w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), _profileTimeout)
	defer cancel()
	if len(parts) == 5 {
		summary, err := g.profileSummary(ctx, scope.Namespace, parts[4])
		if err != nil {
			writeConversationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{_callerSubjectField: scope.Subject,
			_sourceField: g.profileSource(), _profileSourceName: summary})
		return
	}
	limit, after := 100, r.URL.Query().Get("after")
	if values, ok := r.URL.Query()["limit"]; ok {
		parsed, err := strconv.Atoi(values[0])
		if len(values) != 1 || err != nil || parsed < 1 || parsed > 100 {
			http.Error(w, "Invalid Profile page limit", http.StatusBadRequest)
			return
		}
		limit = parsed
	}
	if values, ok := r.URL.Query()["after"]; ok && (len(values) != 1 || after == "" || len(after) > 4096) {
		http.Error(w, "Invalid Profile page cursor", http.StatusBadRequest)
		return
	}
	items, next, err := g.profileSummaries(ctx, scope.Namespace, after, limit)
	if err != nil {
		writeConversationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{_callerSubjectField: scope.Subject,
		_sourceField: g.profileSource(), "profiles": items, _moreField: next != "", "next_cursor": next})
}

func (g *Gateway) profileSummary(ctx context.Context, namespace, name string) (ProfileSummary, error) {
	if g.profiles == nil {
		return ProfileSummary{}, &profileUnavailableError{reason: _profileObservationUnavailable}
	}
	if len(validation.IsDNS1123Subdomain(name)) != 0 {
		return ProfileSummary{}, os.ErrNotExist
	}
	var profile agentsv1alpha1.Profile
	if err := g.profiles.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &profile); err != nil {
		if apierrors.IsNotFound(err) {
			return ProfileSummary{}, os.ErrNotExist
		}
		return ProfileSummary{}, errUnavailable
	}
	_, err := g.profiles.selection(ctx, &profile)
	return summarizeProfile(&profile, err), nil
}

func (g *Gateway) profileSummaries(ctx context.Context, namespace, after string, limit int) ([]ProfileSummary, string, error) {
	if g.profiles == nil {
		return nil, "", &profileUnavailableError{reason: _profileObservationUnavailable}
	}
	items := make([]ProfileSummary, 0, limit)
	var profiles agentsv1alpha1.ProfileList
	if err := g.profiles.reader.List(ctx, &profiles, client.InNamespace(namespace), client.Limit(limit), client.Continue(after)); err != nil {
		return nil, "", errUnavailable
	}
	for i := range profiles.Items {
		profile := &profiles.Items[i]
		if profile.Namespace != namespace {
			return nil, "", errUnavailable
		}
		_, err := g.profiles.selection(ctx, profile)
		items = append(items, summarizeProfile(profile, err))
	}
	return items, profiles.Continue, nil
}
