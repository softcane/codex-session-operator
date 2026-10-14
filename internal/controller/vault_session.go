package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// SessionConfigurationProvider supports durable creation and restricted later-turn updates.
type SessionConfigurationProvider interface {
	CreateConfiguredSession(context.Context, string, string, string, map[string]any, map[string]any, []string, *map[string]string, *agentsv1alpha1.SessionSpendControl) (openaiapi.Session, error)
	UpdateSession(context.Context, string, map[string]any) (openaiapi.Session, error)
}

var _ SessionConfigurationProvider = (*openaiapi.Client)(nil)

const (
	deletePolicy           = "Delete"
	spendModeLimited       = "limited"
	spendModeUnlimited     = "unlimited"
	spendUnavailableReason = "SpendControlUnavailable"
)

func sessionConfigured(s *agentsv1alpha1.Session) bool {
	return s.Spec.Agent != nil || s.Spec.Metadata != nil || len(s.Spec.VaultRefs) > 0 || s.Spec.SpendControl != nil
}

func spendControlDigest(spec *agentsv1alpha1.SessionSpendControl) string {
	if spec == nil {
		return ""
	}
	value := spec.Mode
	if spec.LimitCents != nil {
		value += ":" + strconv.FormatInt(*spec.LimitCents, 10)
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func spendControlMatches(spec *agentsv1alpha1.SessionSpendControl, remote openaiapi.Session) bool {
	if spec == nil || remote.SpendControlMalformed {
		return spec == nil
	}
	if spec.Mode == spendModeUnlimited {
		return remote.SpendLimitCents == nil
	}
	return spec.LimitCents != nil && remote.SpendControlObserved && remote.SpendLimitCents != nil &&
		*remote.SpendLimitCents == *spec.LimitCents
}

type sessionSpendUpdate struct {
	value            any
	present, changed bool
	reason, message  string
}

func decideSessionSpendUpdate(s *agentsv1alpha1.Session, remote openaiapi.Session) sessionSpendUpdate {
	value, present, err := openaiapi.SessionSpendControlField(s.Spec.SpendControl)
	if err != nil {
		return sessionSpendUpdate{reason: invalidConfigurationReason, message: err.Error()}
	}
	decision := sessionSpendUpdate{value: value, present: present}
	if !present || spendControlMatches(s.Spec.SpendControl, remote) {
		return decision
	}
	if remote.SpendControlMalformed {
		decision.reason = "SpendControlUnverified"
		decision.message = "OpenAI returned malformed Session spend control"
		return decision
	}
	if s.Status.SpendControlAttemptedDigest == spendControlDigest(s.Spec.SpendControl) && !remote.SpendControlObserved {
		decision.reason = "SpendControlOutcomeUnknown"
		decision.message = "Spend control update was attempted but OpenAI did not confirm the result"
		if previous := meta.FindStatusCondition(s.Status.Conditions, readyCondition); previous != nil && previous.Reason == spendUnavailableReason {
			decision.reason = spendUnavailableReason
			decision.message = "OpenAI project has not enabled Session spending limits; update claim is retained for audit"
		}
		return decision
	}
	decision.changed = true
	return decision
}

func sessionMetadataUpdate(s *agentsv1alpha1.Session, remote openaiapi.Session) (string, map[string]string, bool, error) {
	scope := s.Status.OwnerScope
	if scope == "" {
		scope = remote.OwnerScope
	}
	metadata, err := openaiapi.VaultMetadata(s.Spec.Metadata, remote.Metadata, s.Status.ManagedMetadataKeys, string(s.UID), scope)
	if err != nil {
		return "", nil, false, err
	}
	// Legacy Sessions have no scope ownership key; preserve their existing identity.
	if scope == "" {
		delete(metadata, "owner_scope")
	}
	return scope, metadata, !reflect.DeepEqual(metadata, remote.Metadata), nil
}
func (r *SessionReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *SessionReconciler) resolveVaults(ctx context.Context, s *agentsv1alpha1.Session, environment *agentsv1alpha1.SessionEnvironmentSnapshot, agent map[string]any) ([]agentsv1alpha1.SessionVaultSnapshot, []string, string, error) {
	var snapshots []agentsv1alpha1.SessionVaultSnapshot
	var ids []string
	var credentials []agentsv1alpha1.VaultCredential
	seen := make(map[string]bool)
	for _, ref := range s.Spec.VaultRefs {
		if seen[ref.Name] {
			return nil, nil, "Vault references must be unique", nil
		}
		seen[ref.Name] = true
		if r.Vaults == nil || r.AgentOwnerScope == "" {
			return nil, nil, "Vault provider and installation/project scope are required", nil
		}
		var v agentsv1alpha1.Vault
		if err := r.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: ref.Name}, &v); err != nil {
			return nil, nil, "Could not read referenced Vault", client.IgnoreNotFound(err)
		}
		ready := meta.FindStatusCondition(v.Status.Conditions, readyCondition)
		if !v.DeletionTimestamp.IsZero() || ready == nil || ready.Status != metav1.ConditionTrue || ready.ObservedGeneration != v.Generation || v.Status.OwnerScope != r.AgentOwnerScope || v.Status.OpenAIVaultID == "" {
			return nil, nil, "Vault must be current, Ready, owned and not deleting", nil
		}
		remote, err := r.Vaults.GetVault(ctx, v.Status.OpenAIVaultID)
		if err != nil || !ownedVault(&v, remote) {
			return nil, nil, "Could not verify remote Vault identity", err
		}
		snapshot := agentsv1alpha1.SessionVaultSnapshot{Name: v.Name, UID: string(v.UID), Generation: v.Generation, OpenAIVaultID: remote.ID}
		selected, frozen, message, err := r.resolveVaultCredentials(ctx, s, &v, remote.ID)
		if message != "" {
			return nil, nil, message, err
		}
		snapshot.Credentials = frozen
		credentials = append(credentials, selected...)

		observed, err := r.Vaults.ListCredentials(ctx, remote.ID)
		if err != nil {
			return nil, nil, "Could not completely observe Vault credential attachments", err
		}
		if !allVaultCredentialsManaged(snapshot.Credentials, observed) {
			return nil, nil, "Vault contains unmanaged credential attachments; audit required", nil
		}

		snapshots = append(snapshots, snapshot)
		ids = append(ids, remote.ID)
	}
	if message := resolveSessionCredentials(credentials, environment, agent); message != "" {
		return nil, nil, message, nil
	}
	return snapshots, ids, "", nil
}

func (r *SessionReconciler) resolveVaultCredentials(ctx context.Context, s *agentsv1alpha1.Session, v *agentsv1alpha1.Vault, vaultID string) ([]agentsv1alpha1.VaultCredential, []agentsv1alpha1.SessionCredentialSnapshot, string, error) {
	var snapshots []agentsv1alpha1.SessionCredentialSnapshot
	var credentials []agentsv1alpha1.VaultCredential
	var list agentsv1alpha1.VaultCredentialList
	if err := r.reader().List(ctx, &list, client.InNamespace(s.Namespace), client.Limit(1000)); err != nil {
		return nil, nil, "Could not list Vault credentials", err
	}
	if list.Continue != "" {
		return nil, nil, "Credential scan exceeded limit", nil
	}
	for _, c := range list.Items {
		if c.Spec.VaultRef.Name != v.Name {
			continue
		}
		ready := meta.FindStatusCondition(c.Status.Conditions, readyCondition)
		if !c.DeletionTimestamp.IsZero() || ready == nil || ready.Status != metav1.ConditionTrue || ready.ObservedGeneration != c.Generation || c.Status.OwnerScope != r.AgentOwnerScope || c.Status.VaultUID != string(v.UID) || c.Status.OpenAIVaultID != vaultID || c.Status.OpenAICredentialID == "" {
			return nil, nil, "Every attached credential must be current, Ready and owned", nil
		}
		secret, versions, err := readCredentialSource(ctx, r.reader(), s.Namespace)
		if err != nil || secret == nil {
			return nil, nil, "Protected credential source is unavailable", client.IgnoreNotFound(err)
		}
		if _, err := openaiapi.CredentialAuth(c.Spec.Auth, secret, false); err != nil {
			return nil, nil, "Protected credential source is invalid", nil
		}
		if !reflect.DeepEqual(versions, c.Status.SourceVersions) || c.Status.ConfigurationDigest != openaiapi.CredentialDigest(c.Spec, versions) {
			return nil, nil, "Credential rotation must be acknowledged before Session creation", nil
		}
		observed, err := r.Vaults.GetCredential(ctx, vaultID, c.Status.OpenAICredentialID)
		if err != nil || !ownedCredential(&c, observed) || !openaiapi.CredentialPublicMatches(c.Spec.Auth, observed) {
			return nil, nil, "Could not verify attached credential public identity", err
		}
		snapshots = append(snapshots, agentsv1alpha1.SessionCredentialSnapshot{Name: c.Name, UID: string(c.UID), Generation: c.Generation, OpenAICredentialID: observed.ID, AuthType: c.Spec.Auth.Type, SourceVersions: slices.Clone(versions)})
		credentials = append(credentials, c)
	}
	return credentials, snapshots, "", nil
}

func allVaultCredentialsManaged(expected []agentsv1alpha1.SessionCredentialSnapshot, observed []openaiapi.Credential) bool {
	if len(expected) != len(observed) {
		return false
	}
	ids := make(map[string]bool, len(expected))
	for _, c := range expected {
		ids[c.OpenAICredentialID] = true
	}
	for _, c := range observed {
		if !ids[c.ID] {
			return false
		}
		delete(ids, c.ID)
	}
	return len(ids) == 0
}

func resolveSessionCredentials(credentials []agentsv1alpha1.VaultCredential, environment *agentsv1alpha1.SessionEnvironmentSnapshot, agent map[string]any) string {
	names := make(map[string]bool)
	byURL := make(map[string][]string)
	selected := make(map[string]string)
	for _, c := range credentials {
		switch {
		case c.Spec.Auth.StaticBearer != nil:
			url := c.Spec.Auth.StaticBearer.ServerURL
			byURL[url] = append(byURL[url], c.Status.OpenAICredentialID)
			selected[c.Status.OpenAICredentialID] = url
		case c.Spec.Auth.MCPOAuth != nil:
			url := c.Spec.Auth.MCPOAuth.ServerURL
			byURL[url] = append(byURL[url], c.Status.OpenAICredentialID)
			selected[c.Status.OpenAICredentialID] = url
		case c.Spec.Auth.EnvironmentVariable != nil:
			if environment == nil || environment.Profile.OpenAIHosted == nil {
				return "Environment credentials require OpenAI-hosted execution; they never supply self-hosted Pod secrets"
			}
			credential := c.Spec.Auth.EnvironmentVariable
			hosted := environment.Profile.OpenAIHosted
			if names[credential.SecretName] {
				return "Attached hosted credentials require unique secret names"
			}
			names[credential.SecretName] = true
			for _, entry := range hosted.Env {
				if entry.Name == credential.SecretName {
					return "Hosted credential name conflicts with Environment env"
				}
			}
			network := hosted.Network
			if network.Access == "disabled" {
				return "Hosted credentials require network access"
			}
			if credential.Networking.Type == "unrestricted" && (network.Access != "restricted" || len(network.AllowedDomains) == 0) {
				return "Unrestricted credential substitution requires restricted Environment network with explicit allowed domains"
			}
			for _, host := range credential.Networking.AllowedHosts {
				if network.Access == "restricted" && !slices.Contains(network.AllowedDomains, host) {
					return "Every credential host must be explicitly allowed by restricted Environment network"
				}
				if slices.Contains(network.BlockedDomains, host) {
					return "Credential host is blocked by Environment network"
				}
			}
		}
	}
	tools, _ := agent["tools"].([]any)
	for _, item := range tools {
		tool, ok := item.(map[string]any)
		if !ok || tool[_wireType] != "mcp" {
			continue
		}
		transport, _ := tool["transport"].(map[string]any)
		id, _ := tool["credential_id"].(string)
		url, _ := transport["server_url"].(string)
		if id != "" && (transport[_wireType] != "http" || tool["connection_origin"] == "environment" || selected[id] != url) {
			return "MCP credential ID must match an attached credential and service-origin HTTP URL"
		}
		if transport[_wireType] == "http" && tool["connection_origin"] != "environment" && id == "" {
			if len(byURL[url]) > 1 {
				return "Several attached credentials match MCP URL; set credentialID explicitly"
			}
			// OpenAI readback resolves a unique matching credential even when the request omitted its selector.
			if len(byURL[url]) == 1 {
				tool["credential_id"] = byURL[url][0]
			}
		}
	}
	return ""
}

func (r *SessionReconciler) configuredAgent(ctx context.Context, s *agentsv1alpha1.Session, snapshot *agentsv1alpha1.SessionAgentSnapshot) (map[string]any, map[string]any, error) {
	saved := map[string]any{"model": openaiapi.SessionModel}
	if snapshot != nil {
		remote, err := r.Agents.GetAgent(ctx, snapshot.OpenAIAgentID)
		if err != nil {
			return nil, nil, err
		}
		if remote.OwnerUID != snapshot.UID || remote.OwnerScope != r.AgentOwnerScope || openaiapi.AgentConfigurationDigest(remote.Config) != snapshot.ConfigurationDigest {
			return nil, nil, errors.New("saved Agent changed during reference resolution")
		}
		saved = remote.Config
	}
	overrides, err := openaiapi.SessionAgentConfiguration(s.Spec.Agent)
	if err != nil {
		return nil, nil, err
	}
	effective := openaiapi.EffectiveSessionAgent(saved, overrides)
	return overrides, effective, nil
}

func (r *SessionReconciler) syncSessionSettings(ctx context.Context, s *agentsv1alpha1.Session, remote openaiapi.Session) (ctrl.Result, bool, error) {
	if remote.OwnerUID != string(s.UID) || remote.ID != s.Status.OpenAISessionID || (s.Status.OwnerScope != "" && remote.OwnerScope != s.Status.OwnerScope) {
		result, err := r.problem(ctx, s, "OwnershipMismatch", "Session update ownership could not be verified", nil)
		return result, true, err
	}
	if s.Status.CreationOnlyDigest == "" && s.Spec.Agent == nil && s.Spec.Metadata == nil && s.Spec.SpendControl == nil {
		return ctrl.Result{}, false, nil
	}
	if r.Configuration == nil {
		result, err := r.problem(ctx, s, "SessionProviderRequired", "Configure supported Session updates", nil)
		return result, true, err
	}
	if s.Status.CreationOnlyDigest == "" {
		// Legacy Sessions can gain mutable settings without being recreated. Capture
		// observed immutable fields before the first permitted update; user creation-only
		// overrides remain differences from their original empty override intent.
		base := s.DeepCopy()
		s.Status.CreationOnlyDigest = openaiapi.SessionCreationOnlyDigest(nil)
		s.Status.CreationAgentDigest = openaiapi.SessionImmutableAgentDigest(remote.AgentConfiguration)
		return ctrl.Result{RequeueAfter: time.Second}, true, r.patchStatus(ctx, s, base)
	}
	changes := make(map[string]any)
	agentChanges := openaiapi.SessionSettingsChanges(s.Spec.Agent, remote, s.Status.AppliedMutableDigest)
	if len(agentChanges) > 0 {
		changes["agent"] = agentChanges
	}
	spend := decideSessionSpendUpdate(s, remote)
	if spend.reason != "" {
		result, problemErr := r.problem(ctx, s, spend.reason, spend.message, nil)
		return result, true, problemErr
	}
	if spend.changed {
		changes["spend_control"] = spend.value
	}
	scope, metadata, metadataChanged, err := sessionMetadataUpdate(s, remote)
	if err != nil {
		result, problemErr := r.problem(ctx, s, invalidConfigurationReason, err.Error(), nil)
		return result, true, problemErr
	}
	if metadataChanged {
		changes["metadata"] = metadata
	}
	if len(changes) == 0 {
		if s.Status.SpendControlAttemptedDigest != "" {
			base := s.DeepCopy()
			s.Status.SpendControlAttemptedDigest = ""
			return ctrl.Result{RequeueAfter: time.Second}, true, r.patchStatus(ctx, s, base)
		}
		return ctrl.Result{}, false, nil
	}
	base := s.DeepCopy()
	if spend.changed {
		s.Status.SpendControlAttemptedDigest = spendControlDigest(s.Spec.SpendControl)
	} else if spend.present && spendControlMatches(s.Spec.SpendControl, remote) {
		s.Status.SpendControlAttemptedDigest = ""
	}
	s.Status.ManagedMetadataKeys = pendingMetadataKeys(s.Spec.Metadata, remote.Metadata, s.Status.ManagedMetadataKeys)
	setState(s, "Updating", metav1.ConditionFalse, "UpdatingSession", "Supported settings for subsequent turns are pending")
	if err := r.patchStatus(ctx, s, base); err != nil {
		return ctrl.Result{}, true, err
	}
	updated, err := r.Configuration.UpdateSession(ctx, remote.ID, changes)
	if err != nil {
		if errors.Is(err, openaiapi.ErrSpendControlUnavailable) {
			result, problemErr := r.problem(ctx, s, spendUnavailableReason, "OpenAI project has not enabled Session spending limits; update claim is retained for audit", err)
			return result, true, problemErr
		}
		result, problemErr := r.problem(ctx, s, "UpdateOutcomeUnknown", "Observe Session before retrying supported updates", err)
		return result, true, problemErr
	}
	if updated.ID != remote.ID || updated.OwnerUID != string(s.UID) || (scope != "" && updated.OwnerScope != scope) {
		result, err := r.problem(ctx, s, "OwnershipMismatch", "Updated Session identity mismatch", nil)
		return result, true, err
	}
	base = s.DeepCopy()
	s.Status.AppliedMutableDigest = openaiapi.SessionMutableDigest(s.Spec.Agent)
	s.Status.ManagedMetadataKeys = appliedMetadataKeys(s.Spec.Metadata, s.Status.ManagedMetadataKeys)
	if spend.present && spendControlMatches(s.Spec.SpendControl, updated) {
		s.Status.SpendControlAttemptedDigest = ""
	}
	return ctrl.Result{RequeueAfter: time.Second}, true, r.patchStatus(ctx, s, base)
}

func phase4AgentMatches(s *agentsv1alpha1.Session, remote openaiapi.Session) bool {
	if s.Status.CreationOnlyDigest == "" {
		return remote.Model == sessionModel(s) && (s.Status.AgentSnapshot == nil || (remote.AgentID == s.Status.AgentSnapshot.OpenAIAgentID && remote.AgentConfigurationDigest == s.Status.AgentSnapshot.ConfigurationDigest))
	}
	return (s.Status.AgentSnapshot == nil || remote.AgentID == s.Status.AgentSnapshot.OpenAIAgentID) && openaiapi.SessionImmutableAgentDigest(remote.AgentConfiguration) == s.Status.CreationAgentDigest
}
func phase4VaultsMatch(s *agentsv1alpha1.Session, remote openaiapi.Session) bool {
	if s.Status.CreationOnlyDigest == "" {
		return true
	}
	expected := make([]string, 0, len(s.Status.VaultSnapshots))
	for _, v := range s.Status.VaultSnapshots {
		expected = append(expected, v.OpenAIVaultID)
	}
	actual := slices.Clone(remote.VaultIDs)
	slices.Sort(expected)
	slices.Sort(actual)
	return slices.Equal(expected, actual)
}
func phase4CreationChanged(s *agentsv1alpha1.Session) bool {
	names := make([]string, 0, len(s.Spec.VaultRefs))
	selected := make([]string, 0, len(s.Status.VaultSnapshots))
	for _, ref := range s.Spec.VaultRefs {
		names = append(names, ref.Name)
	}
	for _, v := range s.Status.VaultSnapshots {
		selected = append(selected, v.Name)
	}
	slices.Sort(names)
	slices.Sort(selected)
	digest := s.Status.CreationOnlyDigest
	if digest == "" {
		digest = openaiapi.SessionCreationOnlyDigest(nil)
	}
	return !slices.Equal(names, selected) || digest != openaiapi.SessionCreationOnlyDigest(s.Spec.Agent)
}
func (r *SessionReconciler) vaultSessionRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	var sessions agentsv1alpha1.SessionList
	if err := r.List(ctx, &sessions, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	name := obj.GetName()
	if c, ok := obj.(*agentsv1alpha1.VaultCredential); ok {
		name = c.Spec.VaultRef.Name
	}
	var result []reconcile.Request
	for i := range sessions.Items {
		s := &sessions.Items[i]
		match := false
		for _, ref := range s.Spec.VaultRefs {
			match = match || ref.Name == name
		}
		for _, v := range s.Status.VaultSnapshots {
			match = match || v.Name == name
		}
		if match {
			result = append(result, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(s)})
		}
	}
	return result
}

// pendingMetadataKeys retains old ownership only while an observed key still exists.
// A committed update with a lost response must not accumulate absent keys forever.
func pendingMetadataKeys(user *map[string]string, remote map[string]string, claimed []string) []string {
	keys := metadataKeys(user)
	for _, key := range claimed {
		if _, present := remote[key]; present {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}
