package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type workStatusFaultReader struct {
	reader client.Reader
}

func (r *workStatusFaultReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("synthetic-private-provider-detail")
}

func (r *workStatusFaultReader) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	return r.reader.List(ctx, list, options...)
}

func TestWorkHistoryRemainsReadableWhenKubernetesObservationFails(t *testing.T) {
	store := testStore(t)
	if err := store.Update(t.Context(), "work-observation", func(r *Record) error {
		r.Namespace = _testNamespace
		r.Work = &Work{ID: r.UID, State: _workOpen, Attempts: []WorkAttempt{
			{Number: 1, SessionName: _testSessionName, SessionUID: _testUID, Phase: _readyCondition},
		}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), _testUID, func(r *Record) error {
		r.Namespace = _testNamespace
		r.Name = _testSessionName
		r.History.ID = _testRemoteID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	g, reader := testGateway(t, store, simulatorForTest())
	g.reader = &workStatusFaultReader{reader: reader}
	response := request(g, http.MethodGet, "/v1/namespaces/team/work/work-observation", _token, "", "")
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "synthetic-private-provider-detail") {
		t.Fatalf("durable work detail unavailable or leaked provider error: %d %s", response.Code, response.Body.String())
	}
	var detail struct {
		Summary      WorkSummary `json:"summary"`
		Conversation Record      `json:"conversation"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if !detail.Summary.ObservationUnavailable || detail.Summary.Phase != _readyCondition ||
		detail.Summary.SessionURL != "" || detail.Conversation.History.ID != _testRemoteID {
		t.Fatalf("lost durable status/history during observation outage: %+v", detail)
	}
	response = request(g, http.MethodGet, "/v1/namespaces/team/work", _token, "", "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"observation_unavailable":true`) {
		t.Fatalf("durable work list unavailable during outage: %d %s", response.Code, response.Body.String())
	}
}

func TestWorkCanCloseAfterTrustedRouteIsRemoved(t *testing.T) {
	store := testStore(t)
	g, writer := testGateway(t, store, simulatorForTest())
	route := WorkRoute{ID: "retired-route", Namespace: _testNamespace, Source: "retired-source",
		Agent: "retired-agent-v1", Environment: "retired-env-v1",
		SpendControl: workerWorkRoute().SpendControl.DeepCopy()}
	if err := g.EnableWork(writer, []WorkRoute{route}); err != nil {
		t.Fatal(err)
	}
	input := WorkInput{RouteID: route.ID, WorkID: "retired-incident", EventID: "retired-task", Kind: _workTask, Text: "Inspect supplied evidence"}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	response := request(g, http.MethodPost, "/v1/namespaces/team/work-events", _token, "", string(raw))
	if response.Code != http.StatusAccepted {
		t.Fatalf("task registration: %d %s", response.Code, response.Body.String())
	}
	g.workRoutes = nil
	input.EventID, input.Kind, input.Text = "retired-close", _workClose, ""
	raw, err = json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	response = request(g, http.MethodPost, "/v1/namespaces/team/work-events", _token, "", string(raw))
	if response.Code != http.StatusAccepted {
		t.Fatalf("retiring route made accepted work impossible to close: %d %s", response.Code, response.Body.String())
	}
	id := workIdentity(route.Namespace, route.ID, route.Source, input.WorkID)
	record, err := store.Get(context.Background(), id)
	if err != nil || record.Work.State != _workClosing {
		t.Fatalf("accepted work not fenced after retirement: %+v %v", record.Work, err)
	}
	input.EventID, input.Kind, input.Text = "retired-restart", _workRestart, "Start again"
	raw, err = json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	response = request(g, http.MethodPost, "/v1/namespaces/team/work-events", _token, "", string(raw))
	if response.Code != http.StatusNotFound {
		t.Fatalf("retired route still authorized new work: %d", response.Code)
	}
}
