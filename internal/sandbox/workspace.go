package sandbox

import (
	"errors"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	WorkspaceSessionUIDLabel    = "agents.softcane.io/session-uid"
	WorkspaceRetainedAnnotation = "agents.softcane.io/workspace-retained"
	WorkspaceStorageEphemeral   = "Ephemeral"
	WorkspaceStoragePersistent  = "Persistent"
	WorkspaceClaimDelete        = "Delete"
	WorkspaceClaimRetain        = "Retain"
)

// WorkspaceClaimNameForUID never reuses a previous Session's claim.
func WorkspaceClaimNameForUID(uid string) string { return "workspace-" + uid }

// PersistentWorkspace returns the frozen storage intent, if any.
func PersistentWorkspace(session *agentsv1alpha1.Session) *agentsv1alpha1.WorkspaceStorage {
	if session.Status.EnvironmentSnapshot == nil || session.Status.EnvironmentSnapshot.Profile.SelfHosted == nil {
		return nil
	}
	storage := session.Status.EnvironmentSnapshot.Profile.SelfHosted.WorkspaceStorage
	if storage == nil || storage.Type != WorkspaceStoragePersistent {
		return nil
	}
	return storage
}

// WorkspaceClaim builds a Session-specific PVC from the frozen Environment profile.
func WorkspaceClaim(session *agentsv1alpha1.Session) (*corev1.PersistentVolumeClaim, error) {
	storage := PersistentWorkspace(session)
	if storage == nil {
		return nil, nil
	}
	if session.UID == "" || len(validation.IsDNS1123Subdomain(storage.StorageClassName)) != 0 ||
		(storage.ClaimDeletionPolicy != WorkspaceClaimDelete && storage.ClaimDeletionPolicy != WorkspaceClaimRetain) {
		return nil, errors.New("frozen persistent workspace configuration is invalid")
	}
	size, err := resource.ParseQuantity(session.Status.EnvironmentSnapshot.Profile.SelfHosted.WorkspaceSize)
	if err != nil || size.Sign() <= 0 || size.Cmp(resource.MustParse("16Gi")) > 0 {
		return nil, errors.New("frozen persistent workspace size is invalid")
	}
	class := storage.StorageClassName
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: WorkspaceClaimNameForUID(string(session.UID)), Namespace: session.Namespace,
			Labels: map[string]string{WorkspaceSessionUIDLabel: string(session.UID)}},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: size}},
			StorageClassName: &class,
		},
	}, nil
}
