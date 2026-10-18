package gateway

import (
	"cmp"
	"context"
	"errors"
	"net/http"
	"os"
	"slices"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type closedRecordSummary struct {
	UID       string `json:"uid"`
	Name      string `json:"name"`
	SourceID  string `json:"source_id,omitempty"`
	Purgeable bool   `json:"purgeable"`
}

type remotePurgeObserver interface {
	GetSession(context.Context, string) (openaiapi.Session, error)
	FindSession(context.Context, string) (openaiapi.Session, error)
}

var _ remotePurgeObserver = (*openaiapi.Client)(nil)

var errRemotePurgeBlocked = errors.New("remote Session absence not proven")

func closedForPurge(record Record) bool {
	return record.Closed || record.Work != nil && record.Work.State == _workClosed
}

func (g *Gateway) handleAdminRecords(w http.ResponseWriter, r *http.Request, parts []string, principal Principal) {
	if !principal.Admin {
		http.Error(w, "Admin role required", http.StatusForbidden)
		return
	}
	if len(parts) == 4 {
		if r.Method != http.MethodGet {
			methodError(w)
			return
		}
		records, err := g.recordSummaries(r.Context(), principal.Namespace, "")
		if err != nil {
			writeError(w, err)
			return
		}
		summaries := summarizeClosedRecords(records, principal.Namespace)
		writeJSON(w, http.StatusOK, map[string]any{_recordsSegment: summaries})
		return
	}
	if len(parts) != 5 || r.Method != http.MethodDelete && r.Method != http.MethodGet {
		methodError(w)
		return
	}
	uid := parts[4]
	if uid == "" || len(uid) > 128 {
		http.NotFound(w, r)
		return
	}
	gate := g.gate(uid)
	gate.Lock()
	defer gate.Unlock()
	record, err := g.store.GetInNamespace(r.Context(), principal.Namespace, uid)
	if errors.Is(err, os.ErrNotExist) || err == nil && record.Namespace != principal.Namespace {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeError(w, err)
		return
	}
	if !closedForPurge(record) {
		http.Error(w, "Record is active", http.StatusConflict)
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, record)
		return
	}
	if record.Work != nil {
		http.Error(w, "Conversation replay record cannot be purged", http.StatusConflict)
		return
	}
	var session agentsv1alpha1.Session
	err = g.reader.Get(r.Context(), client.ObjectKey{Namespace: record.Namespace, Name: record.Name}, &session)
	if err != nil && !apierrors.IsNotFound(err) {
		writeError(w, err)
		return
	}
	if err == nil && string(session.UID) == uid {
		http.Error(w, "Session still exists", http.StatusConflict)
		return
	}
	proofCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := g.verifyRemoteAbsence(proofCtx, record); err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, errRemotePurgeBlocked) {
			status = http.StatusConflict
		}
		http.Error(w, "Remote Session absence not proven", status)
		return
	}
	if err := g.store.Purge(r.Context(), uid); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func summarizeClosedRecords(records []Record, namespace string) []closedRecordSummary {
	summaries := make([]closedRecordSummary, 0, len(records))
	for _, record := range records {
		if record.Namespace != namespace || !closedForPurge(record) {
			continue
		}
		summary := closedRecordSummary{UID: record.UID, Name: record.Name, Purgeable: record.Work == nil}
		if record.Work != nil {
			summary.SourceID = record.Work.ID
		}
		summaries = append(summaries, summary)
	}
	slices.SortFunc(summaries, func(a, b closedRecordSummary) int { return cmp.Compare(a.UID, b.UID) })
	return summaries
}

func (g *Gateway) verifyRemoteAbsence(ctx context.Context, record Record) error {
	observer, ok := g.provider.(remotePurgeObserver)
	if !ok {
		return errUnavailable
	}
	if record.RemoteID != "" {
		if _, err := observer.GetSession(ctx, record.RemoteID); err == nil {
			return errRemotePurgeBlocked
		} else if !errors.Is(err, openaiapi.ErrNotFound) {
			return errUnavailable
		}
	}
	found, err := observer.FindSession(ctx, record.UID)
	if err != nil {
		return errUnavailable
	}
	if found.ID != "" {
		return errRemotePurgeBlocked
	}
	return nil
}
