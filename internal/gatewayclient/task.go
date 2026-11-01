package gatewayclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	_commandList         = "list"
	_commandContinue     = "continue"
	_acceptanceCommitted = "committed"
	_commandTaskStart    = "task-start"
	_commandTaskStop     = "task-stop"
	_commandTask         = "task"
	_commandStop         = "stop"
	_operationFlag       = "--operation"
)

func (r *runner) workflows(ctx context.Context, args []string) (int, error) {
	path := r.client.basePath() + "/workflows"
	switch {
	case len(args) > 0 && args[0] == _commandList:
		flags := commandFlags("workflows list")
		limit := flags.Int("limit", 100, "page size")
		after := flags.String("after", "", "opaque next_cursor from a prior listing")
		if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || *limit < 1 || *limit > 100 {
			return _exitUsage, errors.New("workflows list accepts --limit from 1 to 100 and --after cursor")
		}
		query := url.Values{"limit": {strconv.Itoa(*limit)}}
		if *after != "" {
			query.Set("after", *after)
		}
		path += "?" + query.Encode()
	case len(args) == 2 && args[0] == "show" && validProfileIdentifier(args[1]):
		path += "/" + args[1]
	default:
		return _exitUsage, errors.New("workflows requires list or show NAME")
	}
	return r.readTaskResource(ctx, path)
}

func (r *runner) task(ctx context.Context, args []string) (int, error) {
	if len(args) == 0 {
		return _exitUsage, errors.New("task requires start, read, result, continue or stop")
	}
	switch args[0] {
	case "start", _commandContinue, _commandStop:
		return r.mutate(ctx, "task-"+args[0], args[1:])
	case "result":
		return r.taskResult(ctx, args[1:])
	case "read":
		if len(args) != 2 {
			return _exitUsage, errors.New("task read requires TASK_ID")
		}
		path, err := r.taskPath(args[1])
		if err != nil {
			return _exitUsage, err
		}
		return r.readTaskResource(ctx, path)
	default:
		return _exitUsage, errors.New("unknown task command; use --help")
	}
}

func (r *runner) readTaskResource(ctx context.Context, path string) (int, error) {
	var result json.RawMessage
	if err := r.client.request(ctx, http.MethodGet, path, "", nil, &result); err != nil {
		return _exitFailure, err
	}
	return 0, r.emit(result)
}

func (r *runner) taskPath(id string) (string, error) {
	if !strings.HasPrefix(id, "task-") || !validIdentifier(id) {
		return "", errors.New("task ID required")
	}
	return r.client.basePath() + "/tasks/" + id, nil
}

func (r *runner) taskMutationRequest(command string, args []string) (operation, error) {
	path := r.client.basePath() + "/tasks"
	if command != _commandTaskStart {
		if len(args) == 0 {
			return operation{}, errors.New("task mutation requires TASK_ID")
		}
		var err error
		path, err = r.taskPath(args[0])
		if err != nil {
			return operation{}, err
		}
		args = args[1:]
	}
	flags := commandFlags(command)
	id := flags.String("operation", "", "stable saved operation identity")
	var workflow, textFile string
	if command == _commandTaskStart {
		flags.StringVar(&workflow, "workflow", "", "approved workflow")
	}
	if command != _commandTaskStop {
		flags.StringVar(&textFile, "text-file", "", "task text file or '-' for stdin")
	}
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return operation{}, errors.New("invalid task mutation flags; use --help")
	}
	if *id != "" && !validOperationID(*id) {
		return operation{}, errors.New("invalid operation ID")
	}
	if command == _commandTaskStart && !validProfileIdentifier(workflow) {
		return operation{}, errors.New("task start requires --workflow")
	}
	body := []byte(`{}`)
	if command != _commandTaskStop {
		text, err := r.readText(textFile)
		if err != nil {
			return operation{}, err
		}
		request := map[string]string{"text": text}
		if command == _commandTaskStart {
			request["workflow"] = workflow
		}
		body, _ = json.Marshal(request)
	}
	if len(body) > _maxTextBytes+1024 {
		return operation{}, errors.New("encoded input exceeds the gateway request limit")
	}
	switch command {
	case "task-continue":
		path += "/messages"
	case _commandTaskStop:
		path += "/stop"
	}
	return operation{ID: *id, Command: command, Path: path, Body: body}, nil
}

func (r *runner) validateTaskReceipt(op operation, value receipt) error {
	path, err := r.taskPath(value.TaskID)
	if err != nil || path != value.TaskURL || value.EventID == "" || value.ReceivedAt.IsZero() || value.Acceptance != _acceptanceCommitted {
		return errors.New("task receipt identity invalid; retain and resume the saved operation")
	}
	if op.Command != _commandTaskStart && op.Path != path+"/messages" && op.Path != path+"/stop" {
		return errors.New("task receipt target differs from the saved target; retain the operation")
	}
	return nil
}

const (
	_textFileFlag     = "--text-file"
	_acceptanceField  = "acceptance"
	_commandWorkflows = "workflows"
)

func (r *runner) taskResult(ctx context.Context, args []string) (int, error) {
	if len(args) < 2 || !validIdentifier(args[1]) {
		return _exitUsage, errors.New("task result requires TASK_ID RESULT_ID [--output FILE]")
	}
	path, err := r.taskPath(args[0])
	if err != nil {
		return _exitUsage, err
	}
	flags := commandFlags("task result")
	output := flags.String("output", "", "exclusive destination for verified raw result bytes")
	if flags.Parse(args[2:]) != nil || flags.NArg() != 0 {
		return _exitUsage, errors.New("invalid task result flags")
	}
	path += "/results/" + args[1]
	if *output == "" {
		return r.readTaskResource(ctx, path)
	}
	var result struct {
		Raw    string `json:"raw"`
		SHA256 string `json:"sha256"`
	}
	if err := r.client.request(ctx, http.MethodGet, path, "", nil, &result); err != nil {
		return _exitFailure, err
	}
	sum := sha256.Sum256([]byte(result.Raw))
	if !utf8.ValidString(result.Raw) || result.Raw == "" || result.SHA256 != hex.EncodeToString(sum[:]) {
		return _exitFailure, errors.New("result digest does not match exact UTF-8 raw bytes")
	}
	if err := writeTaskResult(*output, result.Raw); err != nil {
		return _exitFailure, err
	}
	return 0, r.emit(map[string]string{"output": *output, "sha256": result.SHA256})
}

func writeTaskResult(path, raw string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("create exclusive result output file")
	}
	// Do not remove a partial file on failure: its name is now reserved, and it
	// may have been moved or replaced by another process.
	if _, err := file.WriteString(raw); err != nil {
		_ = file.Close()
		return errors.New("write result output file")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errors.New("sync result output file")
	}
	if err := file.Close(); err != nil {
		return errors.New("close result output file")
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("open result output directory")
	}
	defer func() { _ = directory.Close() }()
	if err := directory.Sync(); err != nil {
		return errors.New("sync result output directory")
	}
	return nil
}
