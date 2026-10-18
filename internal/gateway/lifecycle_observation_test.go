package gateway

import (
	"encoding/json"
	"net/http"
	"testing"
)

func readLifecycleDetail(t *testing.T, g *Gateway, url string) ConversationDetail {
	t.Helper()
	response := conversationRequest(g, http.MethodGet, url, "", "")
	var detail ConversationDetail
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &detail) != nil {
		t.Fatal("conversation lifecycle could not be read", response.Code, response.Body.String())
	}
	return detail
}

func TestConversationLifecycleSeparatesRuntimeQueueAndRestorePermission(t *testing.T) {
	store := testStore(t)
	g, _, _ := profileGateway(t, store)
	receipt := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "lifecycle-profile", _profileTestBody))
	detail := readLifecycleDetail(t, g, receipt.ConversationURL)
	if !detail.Lifecycle.DeliveryHeld || detail.Lifecycle.ExecutionReady || detail.Lifecycle.Reason != "Provisioning" ||
		!detail.Lifecycle.Actions.Continue || !detail.Lifecycle.Actions.Close || detail.Lifecycle.Actions.StartFresh {
		t.Fatal("provisioning observation conflated queue permission and execution")
	}
	readConversationReceipt(t, conversationRequest(g, http.MethodPost, receipt.ConversationURL+"/messages", "queued-lifecycle", `{"text":"Queued during preparation"}`))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "lifecycle-profile")
	if err := store.Update(t.Context(), id, func(r *Record) error { r.DeliveryHeld = true; return nil }); err != nil {
		t.Fatal(err)
	}
	detail = readLifecycleDetail(t, g, receipt.ConversationURL)
	if !detail.Lifecycle.DeliveryHeld || detail.Lifecycle.Actions.Continue || detail.Lifecycle.Actions.Close ||
		!detail.Lifecycle.Recovery.Required || detail.Lifecycle.Recovery.Reason != "RestoreReconciliationRequired" {
		t.Fatal("restore hold advertised mutation permission")
	}
	if err := store.Update(t.Context(), id, func(r *Record) error {
		r.DeliveryHeld = false
		r.Work.State = _workClosing
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	detail = readLifecycleDetail(t, g, receipt.ConversationURL)
	if detail.Lifecycle.DeliveryHeld || detail.Lifecycle.ExecutionReady || detail.Lifecycle.Actions.Continue ||
		detail.Lifecycle.Reason != "CleanupPending" || detail.Lifecycle.Actions.StartFresh {
		t.Fatal("cleanup advertised active execution or fresh-attempt permission")
	}
	if err := store.Update(t.Context(), id, func(r *Record) error {
		r.Work.State = _workClosed
		r.Work.Attempts[0].Cleaned = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	detail = readLifecycleDetail(t, g, receipt.ConversationURL)
	if detail.Lifecycle.Reason != "Closed" || !detail.Lifecycle.Actions.StartFresh || detail.Lifecycle.Actions.Close {
		t.Fatal("completed cleanup did not expose explicit start-fresh permission")
	}
}

func TestConversationLifecycleExposesCreationAndDeliveryAuditWithoutClearingClaims(t *testing.T) {
	store := testStore(t)
	g, _, _ := profileGateway(t, store)
	receipt := readConversationReceipt(t, conversationRequest(g, http.MethodPost, _conversationPath, "lifecycle-audit", _profileTestBody))
	id := conversationRecordID(_testNamespace, _testConversationSubject, "lifecycle-audit")
	if err := store.Update(t.Context(), id, func(r *Record) error { r.Work.Attempts[0].CreationClaimed = true; return nil }); err != nil {
		t.Fatal(err)
	}
	detail := readLifecycleDetail(t, g, receipt.ConversationURL)
	if !detail.Lifecycle.Recovery.Required || detail.Lifecycle.Recovery.Reason != "CreationOutcomeUnknown" {
		t.Fatal("ambiguous create lost its structured audit reason")
	}
	if !readWorkRecord(t, store, id).Work.Attempts[0].CreationClaimed {
		t.Fatal("lifecycle read cleared a creation claim")
	}
}
