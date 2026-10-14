package openaiapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

// TestLiveSpendControl checks the beta API contract without starting compute.
// Set LIVE_OPENAI_SPEND=1 and OPENAI_API_KEY to opt into the remote calls.
func TestLiveSpendControl(t *testing.T) {
	if os.Getenv("LIVE_OPENAI_SPEND") != "1" {
		t.Skip("live OpenAI spend-control check is opt-in")
	}
	client, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	uid := "live-spend-" + hex.EncodeToString(nonce[:])
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	limit := int64(500)
	spend := &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeLimited, LimitCents: &limit}
	created, err := client.CreateConfiguredSession(ctx, uid, "live-spend-contract", "",
		map[string]any{typeField: selfHostedType, workspaceDirectoryField: testWorkspace}, nil, nil, nil, spend)
	if err != nil {
		// A lost create response must not be followed by another POST.
		auditCtx, auditCancel := context.WithTimeout(context.Background(), time.Minute)
		defer auditCancel()
		found, findErr := client.FindSession(auditCtx, uid)
		if findErr != nil {
			t.Logf("Could not complete UID discovery for %s: %v", uid, findErr)
		} else if found.ID != "" {
			if cleanupErr := client.DeleteSession(auditCtx, found.ID); cleanupErr != nil {
				t.Errorf("Could not clean up discovered Session %s: %v", found.ID, cleanupErr)
			}
		}
		t.Fatalf("create outcome for UID %s: %v", uid, err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		if err := client.DeleteSession(cleanupCtx, created.ID); err != nil {
			t.Errorf("clean up Session %s: %v", created.ID, err)
		}
	}()
	read, err := client.GetSession(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !read.SpendControlObserved || read.SpendLimitCents == nil || *read.SpendLimitCents != limit {
		t.Fatalf("limited spend readback did not confirm %d cents for Session %s", limit, created.ID)
	}
	next := int64(600)
	if _, err := client.UpdateSession(ctx, created.ID, map[string]any{spendControlField: map[string]any{spendLimitField: next}}); err != nil {
		t.Fatal(err)
	}
	read, err = client.GetSession(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !read.SpendControlObserved || read.SpendLimitCents == nil || *read.SpendLimitCents != next {
		t.Fatalf("updated spend readback did not confirm %d cents for Session %s", next, created.ID)
	}
	if _, err := client.UpdateSession(ctx, created.ID, map[string]any{spendControlField: nil}); err != nil {
		t.Fatal(err)
	}
	read, err = client.GetSession(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if read.SpendControlMalformed || read.SpendLimitCents != nil {
		t.Fatalf("unlimited spend readback retained a limit for Session %s", created.ID)
	}
	if err := client.DeleteSession(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetSession(ctx, created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted Session %s was still retrievable: %v", created.ID, err)
	}
}
