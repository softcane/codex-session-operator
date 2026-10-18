package gatewayclient

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
)

type rootTurn struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type conversationDetail struct {
	Summary struct {
		ConversationID         string `json:"conversation_id"`
		ConversationURL        string `json:"conversation_url"`
		Attempt                int    `json:"attempt"`
		State                  string `json:"state"`
		Phase                  string `json:"phase"`
		Active                 bool   `json:"active"`
		Cleaned                bool   `json:"cleaned"`
		ObservationUnavailable bool   `json:"observation_unavailable"`
	} `json:"summary"`
	History struct {
		ID     string            `json:"id"`
		Status string            `json:"status"`
		Turns  []json.RawMessage `json:"turns"`
	} `json:"history"`
	RecoveryRequired bool `json:"recovery_required"`
	Lifecycle        *struct {
		DeliveryHeld   bool   `json:"delivery_held"`
		ExecutionReady bool   `json:"execution_ready"`
		Reason         string `json:"reason"`
		Recovery       struct {
			Required bool   `json:"required"`
			Reason   string `json:"reason"`
		} `json:"recovery"`
		Actions json.RawMessage `json:"actions"`
	} `json:"lifecycle"`
}

func observedRoots(detail conversationDetail) ([]rootTurn, error) {
	byID := make(map[string]string, len(detail.History.Turns))
	for _, raw := range detail.History.Turns {
		var turn struct {
			ID         string          `json:"id"`
			Status     string          `json:"status"`
			SubagentID json.RawMessage `json:"subagent_id"`
		}
		if json.Unmarshal(raw, &turn) != nil {
			return nil, errors.New("history contains an invalid turn")
		}
		if len(turn.SubagentID) > 0 && string(turn.SubagentID) != "null" && string(turn.SubagentID) != `""` {
			continue
		}
		if turn.ID == "" || turn.Status == "" {
			return nil, errors.New("history contains a root without identity or status")
		}
		if previous, exists := byID[turn.ID]; exists && previous != turn.Status {
			return nil, errors.New("history contains conflicting duplicate roots")
		}
		byID[turn.ID] = turn.Status
	}
	roots := make([]rootTurn, 0, len(byID))
	for id, status := range byID {
		roots = append(roots, rootTurn{ID: id, Status: status})
	}
	slices.SortFunc(roots, func(a, b rootTurn) int { return strings.Compare(a.ID, b.ID) })
	return roots, nil
}

func newRootOutcomes(roots, baseline []rootTurn) []rootTurn {
	previous := make(map[string]bool, len(baseline))
	for _, root := range baseline {
		previous[root.ID] = true
	}
	outcomes := make([]rootTurn, 0)
	for _, root := range roots {
		if !previous[root.ID] && terminalRoot(root.Status) {
			outcomes = append(outcomes, root)
		}
	}
	return outcomes
}

func terminalRoot(status string) bool {
	return status == _rootCompleted || status == "failed" || status == "cancelled"
}

// Lifecycle classification uses structured fields and exact phases, never
// provider or gateway human error strings.
func observationState(detail conversationDetail, roots []rootTurn) string {
	if detail.RecoveryRequired || detail.Lifecycle != nil && detail.Lifecycle.Recovery.Required {
		return "recovery_hold"
	}
	if detail.Summary.ObservationUnavailable {
		return _observationUnavailable
	}
	if detail.Summary.State == _stateClosed || detail.Summary.State == "closing" {
		if !detail.Summary.Cleaned {
			return "cleanup"
		}
		return _stateClosed
	}
	if detail.Lifecycle != nil && detail.Lifecycle.DeliveryHeld {
		return "recovery_hold"
	}
	if detail.Lifecycle != nil {
		switch detail.Lifecycle.Reason {
		case "DeliveryObservationUnavailable", "SessionObservationUnavailable", "RuntimeObservationUnavailable",
			"ExecutorObservationUnavailable":
			return _observationUnavailable
		case "EnvironmentConnecting":
			return "connecting"
		case "Provisioning", "WorkspacePreparing", "SpendControlPending":
			return _statePreparing
		}
		if !detail.Lifecycle.ExecutionReady {
			return _statePreparing
		}
	}
	for _, root := range roots {
		if root.Status == "in_progress" || root.Status == "waiting" {
			return "running"
		}
	}
	switch detail.Summary.Phase {
	case "Connecting":
		return "connecting"
	case "Ready":
		return _stateAccepted
	case "Pending", "Provisioning", "Preparing":
		return _statePreparing
	}
	if detail.History.ID == "" {
		return _observationUnavailable
	}
	return _stateAccepted
}
