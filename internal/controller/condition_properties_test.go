package controller

import (
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSessionConditionProperties(t *testing.T) {
	tests := []struct {
		name, reason string
		remote       metav1.ConditionStatus
		revoke       bool
	}{
		{name: "supported update", reason: "UpdatingSession", remote: metav1.ConditionTrue},
		{name: "uncertain update", reason: "UpdateOutcomeUnknown", remote: metav1.ConditionTrue},
		{name: "observation outage", reason: "ObservationFailed", remote: metav1.ConditionUnknown},
		{name: "remote absence", reason: remoteSessionMissingReason, remote: metav1.ConditionFalse, revoke: true},
		{name: "ownership mismatch", reason: ownershipMismatchReason, remote: metav1.ConditionUnknown, revoke: true},
		{name: "unsafe configuration", reason: configurationMismatchReason, remote: metav1.ConditionTrue, revoke: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{Generation: 2}, Status: agentsv1alpha1.SessionStatus{OpenAISessionID: "sess-owned", WorkspacePreparation: &agentsv1alpha1.SessionWorkspacePreparationStatus{ConfigDigest: "prepared"}, Conditions: []metav1.Condition{
				{Type: "RemoteSessionCreated", Status: metav1.ConditionTrue, Reason: "RemoteSessionCreated", ObservedGeneration: 2},
				{Type: workspacePreparedCondition, Status: metav1.ConditionTrue, Reason: "Prepared", ObservedGeneration: 2},
				{Type: connectedCondition, Status: metav1.ConditionTrue, Reason: "Connected", ObservedGeneration: 2},
				{Type: "FutureCondition", Status: metav1.ConditionTrue, Reason: "FutureReason", ObservedGeneration: 2},
			}}}
			setState(s, metav1.ConditionFalse, tt.reason, "test observation")
			remote := meta.FindStatusCondition(s.Status.Conditions, "RemoteSessionCreated")
			if remote == nil || remote.Status != tt.remote || remote.ObservedGeneration != s.Generation {
				t.Fatalf("incorrect remote observation: %#v", remote)
			}
			if (s.Status.WorkspacePreparation == nil) != tt.revoke {
				t.Fatal("workspace proof revocation differs")
			}
			if !tt.revoke && (!meta.IsStatusConditionTrue(s.Status.Conditions, workspacePreparedCondition) || !meta.IsStatusConditionTrue(s.Status.Conditions, connectedCondition)) {
				t.Fatal("unrelated update changed compute properties")
			}
			if !meta.IsStatusConditionTrue(s.Status.Conditions, "FutureCondition") {
				t.Fatal("unknown condition was removed")
			}
		})
	}
}

func TestSessionRemoteAbsenceSurvivesUnrelatedErrors(t *testing.T) {
	s := &agentsv1alpha1.Session{Status: agentsv1alpha1.SessionStatus{OpenAISessionID: "retained-for-audit"}}
	setState(s, metav1.ConditionFalse, remoteSessionMissingReason, "not found")
	setState(s, metav1.ConditionFalse, "WorkspaceCleanupPending", "cleanup remains pending")
	if !meta.IsStatusConditionFalse(s.Status.Conditions, "RemoteSessionCreated") {
		t.Fatal("retained identity resurrected a missing remote Session")
	}
}
