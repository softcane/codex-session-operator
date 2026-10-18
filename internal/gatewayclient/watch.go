package gatewayclient

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type watchBaseline struct {
	roots      []rootTurn
	known, set bool
	attempt    int
}

type watchOptions struct {
	path, operationID, rootID string
	poll                      time.Duration
}

func (r *runner) watch(ctx context.Context, args []string) (int, error) {
	if len(args) == 0 {
		return _exitUsage, errors.New("watch requires one conversation ID or URL")
	}
	path, err := r.conversationPath(args[0])
	if err != nil {
		return _exitUsage, err
	}
	flags := commandFlags("watch")
	operationID := flags.String("operation", "", "saved mutation baseline")
	rootID := flags.String("root-id", "", "explicit root identity to observe")
	timeout := flags.Duration("timeout", 5*time.Minute, "bounded observation duration")
	poll := flags.Duration("poll", 2*time.Second, "poll interval")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
		return _exitUsage, errors.New("invalid watch flags")
	}
	if *timeout <= 0 || *poll <= 0 || *operationID != "" && *rootID != "" {
		return _exitUsage, errors.New("watch requires positive timeout/poll and at most one operation or root identity")
	}
	options := watchOptions{path: path, operationID: *operationID, rootID: *rootID, poll: *poll}
	baseline, code, err := r.loadWatchBaseline(ctx, options)
	if err != nil {
		return code, err
	}
	watchCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	return r.watchLoop(watchCtx, options, baseline)
}

func (r *runner) loadWatchBaseline(ctx context.Context, options watchOptions) (watchBaseline, int, error) {
	baseline := watchBaseline{roots: make([]rootTurn, 0)}
	if options.operationID == "" {
		return baseline, 0, nil
	}
	j, err := openJournal(r.stateDir)
	if err != nil {
		return baseline, _exitFailure, err
	}
	defer j.close()
	op, err := j.load(options.operationID)
	if err != nil {
		return baseline, _exitFailure, errors.New("read saved observation baseline")
	}
	subject, err := r.client.subject(ctx)
	if err != nil {
		return baseline, _exitFailure, err
	}
	if op.Origin != r.client.origin || op.Namespace != r.client.namespace || op.Subject != subject {
		return baseline, _exitFailure, errors.New("observation baseline differs from origin, namespace or authenticated subject")
	}
	accepted, err := j.loadReceipt(op.ID)
	if err != nil {
		return baseline, _exitAmbiguous, errors.New("operation has no durable receipt; resume it before watching")
	}
	if err := r.validateReceipt(op, accepted); err != nil {
		return baseline, _exitAmbiguous, err
	}
	if accepted.ConversationURL != options.path {
		return baseline, _exitFailure, errors.New("observation target differs from the saved receipt")
	}
	baseline = watchBaseline{roots: op.Baseline, known: op.BaselineKnown, set: true, attempt: accepted.Attempt}
	// A fresh attempt never adopts root identities from the old attempt.
	if op.Command == _commandFresh {
		baseline.roots, baseline.known = make([]rootTurn, 0), false
	}
	return baseline, 0, nil
}

func (r *runner) watchLoop(ctx context.Context, options watchOptions, baseline watchBaseline) (int, error) {
	last := ""
	for {
		detail, readErr := r.detail(ctx, options.path)
		if readErr != nil {
			if ctx.Err() != nil {
				return r.pending(options.operationID, "observation timeout; operation retained")
			}
			if err := r.emit(map[string]any{_stateField: _observationUnavailable, _operationIDField: options.operationID,
				_correlationField: _correlationUnavailable}); err != nil {
				return _exitFailure, err
			}
			return _exitFailure, readErr
		}
		if baseline.attempt != 0 && detail.Summary.Attempt != baseline.attempt {
			return _exitAmbiguous, errors.New("observed attempt differs from the saved receipt")
		}
		roots, rootErr := observedRoots(detail)
		if !baseline.set {
			baseline.roots, baseline.known, baseline.set = roots, rootErr == nil && detail.History.ID != "", true
		}
		state := observationState(detail, roots)
		outcomes := newRootOutcomes(roots, baseline.roots)
		result := observationResult(detail, options, baseline, roots, outcomes)
		if rootErr != nil {
			result[_stateField], result["reason"] = "ambiguous", "invalid or inconsistent root snapshot"
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return _exitFailure, errors.New("encode observation")
		}
		if string(encoded) != last {
			if err := r.emit(result); err != nil {
				return _exitFailure, err
			}
			last = string(encoded)
		}
		if options.rootID != "" && rootErr == nil {
			code, found, err := r.explicitRootOutcome(roots, options.rootID)
			if found || err != nil {
				return code, err
			}
		}
		if len(outcomes) > 0 && options.rootID == "" {
			return _exitAmbiguous, nil
		}
		if state == _stateClosed && options.rootID == "" {
			return 0, nil
		}
		timer := time.NewTimer(options.poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return r.pending(options.operationID, "observation timeout; operation retained")
		case <-timer.C:
		}
	}
}

func observationResult(detail conversationDetail, options watchOptions, baseline watchBaseline, roots, outcomes []rootTurn) map[string]any {
	result := map[string]any{_stateField: observationState(detail, roots), "conversation_url": options.path,
		"attempt": detail.Summary.Attempt, "roots": roots, "new_root_outcomes": outcomes,
		"baseline_observed": baseline.known, _correlationField: _correlationUnavailable, "phase": detail.Summary.Phase}
	if options.operationID != "" {
		result[_operationIDField] = options.operationID
	}
	if detail.Lifecycle != nil {
		result["lifecycle"] = detail.Lifecycle
	}
	if len(outcomes) > 0 {
		result[_stateField] = "observed_root_outcome"
		result["reason"] = "observed terminal roots cannot be attributed to this input"
	}
	return result
}

func (r *runner) explicitRootOutcome(roots []rootTurn, id string) (int, bool, error) {
	for _, root := range roots {
		if root.ID != id || !terminalRoot(root.Status) {
			continue
		}
		if err := r.emit(map[string]any{_stateField: "observed_root_outcome", "root": root,
			_correlationField: _correlationUnavailable}); err != nil {
			return _exitFailure, true, err
		}
		if root.Status != _rootCompleted {
			return _exitRootFailed, true, nil
		}
		return 0, true, nil
	}
	return 0, false, nil
}

func (r *runner) pending(id, reason string) (int, error) {
	return _exitPending, r.emit(map[string]any{_stateField: "pending", "reason": reason,
		_operationIDField: id, _correlationField: _correlationUnavailable})
}
