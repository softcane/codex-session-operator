package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
)

const (
	_reservationRoute  = "reservation-route"
	_reservationSource = "quota-test"
)

func TestWorkCapacityReservesAttemptBeforeAcceptance(t *testing.T) {
	store, _ := postgresForTestLimits(t, 32, 32)
	for i := range 32 - 1 {
		if err := store.Update(t.Context(), fmt.Sprintf("conversation-%d", i), func(r *Record) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Update(t.Context(), "work-new", func(r *Record) error {
		r.Work = &Work{ID: r.UID, RouteID: _reservationRoute, Source: _reservationSource, ExternalID: r.UID, State: _workOpen, Attempts: []WorkAttempt{{Number: 1}}}
		return nil
	}); err == nil {
		t.Fatal("accepted work without room for its future conversation")
	}
	if _, err := store.Get(t.Context(), "work-new"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected work was persisted: %v", err)
	}
}

func TestWorkRestartCapacityIsCheckedOnExistingRecord(t *testing.T) {
	store, _ := postgresForTestLimits(t, 32, 32)
	if err := store.Update(t.Context(), "work-retained", func(r *Record) error {
		r.Work = &Work{ID: r.UID, RouteID: _reservationRoute, Source: _reservationSource, ExternalID: r.UID, State: _workClosed, Attempts: []WorkAttempt{{Number: 1, Cleaned: true}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := range 32 - 1 {
		if err := store.Update(t.Context(), fmt.Sprintf("conversation-%d", i), func(r *Record) error { r.Closed = true; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	restart := func(r *Record) error {
		r.Work.State = _workOpen
		r.Work.Attempts = append(r.Work.Attempts, WorkAttempt{Number: 2})
		return nil
	}
	if err := store.Update(t.Context(), "work-retained", restart); err == nil {
		t.Fatal("restart bypassed reservation on an existing work record")
	}
	record, err := store.Get(context.Background(), "work-retained")
	if err != nil {
		t.Fatal(err)
	}
	if record.Work.State != _workClosed || len(record.Work.Attempts) != 1 {
		t.Fatal("failed restart changed the durable work record")
	}
	if err := store.Purge(t.Context(), "conversation-0"); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), "work-retained", restart); err != nil {
		t.Fatalf("restart with capacity available: %v", err)
	}
	if err := store.Update(t.Context(), "work-retained", func(r *Record) error {
		r.Work.Attempts[1].SessionUID = "new-conversation"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), "new-conversation", func(r *Record) error { return nil }); err != nil {
		t.Fatalf("bound conversation consumed a second reservation: %v", err)
	}
}

func TestWorkReplayTombstoneCannotBePurged(t *testing.T) {
	store := testStore(t)
	if err := store.Update(t.Context(), "work-replay", func(r *Record) error {
		r.Namespace = _testNamespace
		r.Work = &Work{ID: r.UID, RouteID: _reservationRoute, Source: _reservationSource, ExternalID: r.UID, State: _workClosed}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Purge(t.Context(), "work-replay"); err == nil {
		t.Fatal("purged replay tombstone directly")
	}
	g, _ := testGateway(t, store, simulatorForTest())
	g.principals[0].Admin = true
	response := request(g, http.MethodDelete, "/v1/namespaces/team/records/work-replay", _token, "", "")
	if response.Code != http.StatusConflict {
		t.Fatalf("admin purge removed replay tombstone: %d %s", response.Code, response.Body.String())
	}
	if _, err := store.Get(t.Context(), "work-replay"); err != nil {
		t.Fatal(err)
	}
}
