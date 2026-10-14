package sandbox

import (
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const approvedStorageClass = "approved"

func TestPersistentWorkspacePodAndClaim(t *testing.T) {
	image := "example.com/executor@sha256:" + strings.Repeat("a", 64)
	profile := &agentsv1alpha1.SelfHostedEnvironment{
		Image: image, Architecture: "amd64", WorkspaceDirectory: defaultWorkspaceDirectory, WorkspaceSize: "2Gi",
		WorkspaceStorage: &agentsv1alpha1.WorkspaceStorage{Type: WorkspaceStoragePersistent, StorageClassName: approvedStorageClass, ClaimDeletionPolicy: WorkspaceClaimRetain},
	}
	if err := ValidateProfile(profile, []string{image}, nil, []string{approvedStorageClass}); err != nil {
		t.Fatal(err)
	}
	session := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{UID: "session-uid", Namespace: "tenant"},
		Status: agentsv1alpha1.SessionStatus{ExecutorImage: image, EnvironmentSnapshot: &agentsv1alpha1.SessionEnvironmentSnapshot{
			Profile: agentsv1alpha1.EnvironmentSpec{Mode: "selfHosted", SelfHosted: profile}}}}
	claim, err := WorkspaceClaim(session)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Name != WorkspaceClaimNameForUID("session-uid") || *claim.Spec.StorageClassName != approvedStorageClass ||
		len(claim.Spec.AccessModes) != 1 || claim.Spec.AccessModes[0] != corev1.ReadWriteOnce ||
		claim.Spec.Resources.Requests.Storage().String() != "2Gi" {
		t.Fatal("persistent workspace claim contract changed")
	}
	pod, err := Pod(session, defaultWorkspaceDirectory, "")
	if err != nil {
		t.Fatal(err)
	}
	volume := pod.Spec.Volumes[0]
	if volume.EmptyDir != nil || volume.PersistentVolumeClaim == nil || volume.PersistentVolumeClaim.ClaimName != claim.Name ||
		pod.Spec.Containers[0].VolumeMounts[0].MountPath != defaultWorkspaceDirectory {
		t.Fatal("executor did not mount the frozen Session claim")
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("persistent workspace changed executor credentials")
	}
	other := session.DeepCopy()
	other.UID = "other-session-uid"
	otherClaim, err := WorkspaceClaim(other)
	if err != nil || otherClaim.Name == claim.Name {
		t.Fatal("recreated Session reused a previous claim")
	}
}

func TestPersistentWorkspacePolicyRestrictions(t *testing.T) {
	tests := []struct {
		name    string
		storage *agentsv1alpha1.WorkspaceStorage
		classes []string
		valid   bool
	}{
		{name: "legacy", valid: true},
		{name: "explicit ephemeral", storage: &agentsv1alpha1.WorkspaceStorage{Type: WorkspaceStorageEphemeral}, valid: true},
		{name: "approved persistent", storage: &agentsv1alpha1.WorkspaceStorage{Type: WorkspaceStoragePersistent, StorageClassName: approvedStorageClass, ClaimDeletionPolicy: WorkspaceClaimDelete}, classes: []string{approvedStorageClass}, valid: true},
		{name: "missing class approval", storage: &agentsv1alpha1.WorkspaceStorage{Type: WorkspaceStoragePersistent, StorageClassName: approvedStorageClass, ClaimDeletionPolicy: WorkspaceClaimDelete}},
		{name: "wildcard approval", storage: &agentsv1alpha1.WorkspaceStorage{Type: WorkspaceStoragePersistent, StorageClassName: approvedStorageClass, ClaimDeletionPolicy: WorkspaceClaimDelete}, classes: []string{"*"}},
		{name: "implicit policy", storage: &agentsv1alpha1.WorkspaceStorage{Type: WorkspaceStoragePersistent, StorageClassName: approvedStorageClass}, classes: []string{approvedStorageClass}},
		{name: "ephemeral with class", storage: &agentsv1alpha1.WorkspaceStorage{Type: WorkspaceStorageEphemeral, StorageClassName: approvedStorageClass}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if (ValidateWorkspaceStorage(tt.storage, tt.classes) == nil) != tt.valid {
				t.Fatal("workspace storage policy validation mismatch")
			}
		})
	}
}
