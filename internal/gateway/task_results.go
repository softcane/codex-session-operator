package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode/utf8"
)

var errWorkflowInput = errors.New("invalid workflow input")

func workflowInputError(reason string) error { return fmt.Errorf("%w: %s", errWorkflowInput, reason) }

func taskDigest(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func decodeWorkflowArguments(raw json.RawMessage, target any) error {
	if len(raw) > _maxTaskResultBytes || !utf8.Valid(raw) {
		return workflowInputError("workflow arguments exceed 256 KiB or are not UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return workflowInputError("invalid workflow arguments")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return workflowInputError("one JSON value required")
	}
	return nil
}

func validateWorkflowResult(task Task, member TaskMember, raw json.RawMessage, history []json.RawMessage) (WorkflowResultInput, error) {
	var input WorkflowResultInput
	if err := decodeWorkflowArguments(raw, &input); err != nil {
		return input, err
	}
	if !slices.Contains([]string{_taskOutcomeSuccess, "failed", "cancelled", "incomplete"}, input.Outcome) {
		return input, workflowInputError("invalid result outcome")
	}
	names := map[string]bool{}
	for _, output := range input.Outputs {
		if strings.TrimSpace(output.Name) == "" || names[output.Name] || strings.TrimSpace(output.Content) == "" {
			return input, workflowInputError("unique named nonempty outputs required")
		}
		names[output.Name] = true
		sum := sha256.Sum256([]byte(output.Content))
		if output.SHA256 != "" && output.SHA256 != hex.EncodeToString(sum[:]) {
			return input, workflowInputError("output digest mismatch")
		}
		if output.Kind == "patch" && (!workflowExactGitRevision(output.BaseRevision) || output.SHA256 == "" || len(output.Checks) == 0 || output.Limitations == "") {
			return input, workflowInputError("patch requires base revision, digest, checks and limitations")
		}
		for _, check := range output.Checks {
			if check.ExitCode == nil || strings.TrimSpace(check.Command) == "" || check.SourceItemID == "" || !workflowCheckEvidence(history, check) {
				return input, workflowInputError("check must match a saved command execution observation")
			}
		}
	}
	if input.Outcome == _taskOutcomeSuccess && !member.IsCoordinator {
		for _, role := range task.Workflow.Spec.Roles {
			if role.Name == member.Role {
				for _, name := range role.Outputs {
					if !names[name] {
						return input, workflowInputError("successful result missing required output")
					}
				}
			}
		}
	}
	for _, reference := range input.References {
		if _, ok := taskResult(task, reference); !ok {
			return input, workflowInputError("result reference is not saved in this task")
		}
	}
	return input, nil
}

func workflowCheckEvidence(items []json.RawMessage, check WorkflowCheck) bool {
	for _, raw := range items {
		var item struct {
			ID       string `json:"id"`
			Type     string `json:"type"`
			Command  string `json:"command"`
			Output   string `json:"output"`
			ExitCode *int   `json:"exit_code"`
			Status   string `json:"status"`
		}
		if json.Unmarshal(raw, &item) == nil && item.ID == check.SourceItemID {
			return check.ExitCode != nil && (item.Status == "completed" || item.Status == "failed") && item.Type == "command_execution" && item.Command == check.Command && item.Output == check.Output && item.ExitCode != nil && *item.ExitCode == *check.ExitCode
		}
	}
	return false
}

func taskResult(task Task, id string) (TaskResult, bool) {
	for _, result := range task.Results {
		if result.ID == id {
			return result, true
		}
	}
	return TaskResult{}, false
}

func taskMember(task Task, id string) (TaskMember, bool) {
	for _, member := range task.Members {
		if member.ID == id {
			return member, true
		}
	}
	return TaskMember{}, false
}

func workflowExactGitRevision(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
