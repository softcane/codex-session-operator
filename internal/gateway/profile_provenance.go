package gateway

import (
	"context"
	"errors"
	"os"
	"slices"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ValidateSessionPolicy fences remote creation against restore holds and
// verifies the Session still belongs to its frozen Profile attempt.
func (g *Gateway) ValidateSessionPolicy(
	ctx context.Context,
	session *agentsv1alpha1.Session,
	environment *agentsv1alpha1.SessionEnvironmentSnapshot,
) error {
	if err := g.deliveryEnabled(ctx); err != nil {
		return err
	}
	record, err := g.store.Get(ctx, string(session.UID))
	if err == nil && record.DeliveryHeld {
		return errRestoreHeld
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return g.validateWorkSession(ctx, session, environment)
}

func copyProfileProvenance(profile *ProfileProvenance) *ProfileProvenance {
	if profile == nil {
		return nil
	}
	copy := *profile
	copy.Vaults = slices.Clone(profile.Vaults)
	return &copy
}

func bindProfile(record *Record, profile *ProfileProvenance) {
	record.Work.Attempts[len(record.Work.Attempts)-1].Profile = copyProfileProvenance(profile)
}

func validateProfileProvenance(record Record, attempt WorkAttempt) error {
	profile := attempt.Profile
	if profile == nil {
		return errConflict
	}
	policy := attempt.Policy
	if record.Work == nil || record.Work.Conversation == nil || profile.Namespace != record.Namespace ||
		profile.Name != record.Work.RouteID || len(validation.IsDNS1123Subdomain(profile.Name)) != 0 ||
		profile.UID == "" || len(profile.UID) > 128 || profile.Generation < 1 ||
		profile.ResourceVersion == "" || len(profile.ResourceVersion) > 4096 ||
		policy.AgentRef == nil || policy.EnvironmentRef == nil || policy.SpendControl == nil ||
		len(profile.Vaults) != len(policy.VaultRefs) || len(profile.Vaults) > 16 {
		return errConflict
	}
	validDependency := func(expected agentsv1alpha1.ProfileDependencyStatus, name string) bool {
		return expected.Name == name && expected.UID != "" && len(expected.UID) <= 128 &&
			expected.Generation > 0 && len(validation.IsDNS1123Subdomain(name)) == 0
	}
	if !validDependency(profile.Agent, policy.AgentRef.Name) ||
		!validDependency(profile.Environment, policy.EnvironmentRef.Name) {
		return errConflict
	}
	byName := make(map[string]agentsv1alpha1.ProfileDependencyStatus, len(profile.Vaults))
	for _, vault := range profile.Vaults {
		if _, exists := byName[vault.Name]; exists {
			return errConflict
		}
		byName[vault.Name] = vault
	}
	for _, ref := range policy.VaultRefs {
		if !validDependency(byName[ref.Name], ref.Name) {
			return errConflict
		}
		delete(byName, ref.Name)
	}
	spec := agentsv1alpha1.ProfileSpec{AgentRef: *policy.AgentRef, EnvironmentRef: *policy.EnvironmentRef,
		VaultRefs: policy.VaultRefs, SpendControl: *policy.SpendControl}
	if profile.PolicyDigest != agentsv1alpha1.ProfilePolicyDigest(spec) {
		return errConflict
	}
	return nil
}

// validateProfileDependencies runs for both early policy validation and the
// controller's second check against its actual resolved snapshots before claim.
// After freezing, missing current dependencies cannot replace the pinned source.
func (g *Gateway) validateProfileDependencies(ctx context.Context, session *agentsv1alpha1.Session,
	environment *agentsv1alpha1.SessionEnvironmentSnapshot, profile *ProfileProvenance) error {
	if profile == nil {
		return &profileUnavailableError{reason: _dependencyIdentityMismatchReason}
	}
	if environment == nil || environment.Name != profile.Environment.Name || environment.UID != profile.Environment.UID {
		return &profileUnavailableError{reason: _dependencyIdentityMismatchReason}
	}
	if snapshot := session.Status.AgentSnapshot; snapshot != nil {
		if snapshot.Name != profile.Agent.Name || snapshot.UID != profile.Agent.UID {
			return &profileUnavailableError{reason: _dependencyIdentityMismatchReason}
		}
	} else {
		var agent agentsv1alpha1.Agent
		if err := g.expectedDependencyUID(ctx, session.Namespace, profile.Agent, &agent); err != nil {
			return err
		}
	}
	if len(session.Status.VaultSnapshots) > 0 {
		if len(session.Status.VaultSnapshots) != len(profile.Vaults) {
			return &profileUnavailableError{reason: _dependencyIdentityMismatchReason}
		}
		for _, expected := range profile.Vaults {
			found := false
			for _, snapshot := range session.Status.VaultSnapshots {
				if snapshot.Name == expected.Name && snapshot.UID == expected.UID {
					found = true
				}
			}
			if !found {
				return &profileUnavailableError{reason: _dependencyIdentityMismatchReason}
			}
		}
	} else {
		for _, expected := range profile.Vaults {
			var vault agentsv1alpha1.Vault
			if err := g.expectedDependencyUID(ctx, session.Namespace, expected, &vault); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *Gateway) expectedDependencyUID(ctx context.Context, namespace string,
	expected agentsv1alpha1.ProfileDependencyStatus, object client.Object) error {
	if err := g.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: expected.Name}, object); err != nil {
		return &profileUnavailableError{reason: "DependencyMissing"}
	}
	if string(object.GetUID()) != expected.UID || !object.GetDeletionTimestamp().IsZero() {
		return &profileUnavailableError{reason: _dependencyIdentityMismatchReason}
	}
	return nil
}

func profileAttemptDigests(record Record) []string {
	if record.Work == nil {
		return nil
	}
	digests := make([]string, len(record.Work.Attempts))
	for i, attempt := range record.Work.Attempts {
		if attempt.Profile != nil {
			digests[i] = digestValue([]any{attempt.Profile, attempt.Policy})
		}
	}
	return digests
}
