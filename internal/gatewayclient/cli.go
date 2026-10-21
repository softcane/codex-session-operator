package gatewayclient

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	_exitFailure            = 1
	_exitUsage              = 2
	_exitPending            = 3
	_exitAmbiguous          = 4
	_exitRootFailed         = 5
	_maxTextBytes           = 256 << 10
	_commandClose           = "close"
	_commandFresh           = "start-fresh"
	_stateClosed            = "closed"
	_stateAccepted          = "accepted"
	_correlationField       = "input_turn_correlation"
	_observationUnavailable = "observation_unavailable"
	_rootCompleted          = "completed"
	_namespaceField         = "namespace"
)

const (
	_commandStart           = "start"
	_operationIDField       = "operation_id"
	_correlationUnavailable = "unavailable"
	_callerSubjectField     = "caller_subject"
)

const _stateField = "state"

const _statePreparing = "preparing"

const _usage = `gateway-client --origin HTTP_OR_HTTPS_ORIGIN --namespace NS [--ca-file FILE] [--state-dir DIR] COMMAND

Commands:
  profiles [list [--limit 100] | show NAME]  Discover approved profiles
  list [--limit 100] [--after CURSOR]       List conversation attempts
  start --profile NAME --text-file FILE    Start a conversation
  read CONVERSATION                       Read saved history and lifecycle
  watch CONVERSATION [--operation ID | --root-id ID] [--timeout 5m] [--poll 2s]
  continue CONVERSATION --text-file FILE   Continue the selected attempt
  close CONVERSATION                       Request close and cleanup
  start-fresh CONVERSATION --text-file FILE Start a new attempt after cleanup
  resume OPERATION_ID                      Resume an immutable saved operation
  operations                              List private saved operation identities

Mutation options: --operation ID (optional stable local identity); start also accepts --title TITLE.
Text files accept '-' for stdin. Global flags precede COMMAND; mutation flags follow CONVERSATION.
Mutations require --state-dir. Keep this private directory to recover lost responses.
HTTP 202 confirms durable acceptance. History names observed root outcomes; input-to-turn correlation is unavailable.
Exit codes: 0 accepted/read/explicit root completed; 1 rejected/error; 2 usage; 3 pending timeout;
4 uncertain acceptance or uncorrelated observed outcome; 5 explicit root failed/cancelled.
`

type runner struct {
	client     *client
	stateDir   string
	in         io.Reader
	out        io.Writer
	diagnostic io.Writer
}

// Run is a testable command entry point with a private durable operation journal.
func Run(ctx context.Context, args []string, in io.Reader, out, diagnostic io.Writer) int {
	flags := flag.NewFlagSet("gateway-client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var config clientConfig
	var stateDir string
	flags.StringVar(&config.origin, "origin", "", "explicit HTTP or HTTPS origin")
	flags.StringVar(&config.namespace, _namespaceField, "", "target Kubernetes namespace")
	flags.StringVar(&config.caFile, "ca-file", "", "trusted CA PEM file")
	flags.DurationVar(&config.requestTimeout, "request-timeout", 15*time.Second, "bounded HTTP request timeout")
	flags.StringVar(&stateDir, "state-dir", "", "private durable operation directory")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = io.WriteString(out, _usage)
			return 0
		}
		_, _ = io.WriteString(diagnostic, "invalid global flags; use --help\n")
		return _exitUsage
	}
	remaining := flags.Args()
	if len(remaining) == 0 || remaining[0] == "help" {
		_, _ = io.WriteString(out, _usage)
		return 0
	}
	c, err := newClient(config)
	if err != nil {
		_, _ = fmt.Fprintln(diagnostic, err)
		return _exitFailure
	}
	defer c.close()
	r := &runner{client: c, stateDir: stateDir, in: in, out: out, diagnostic: diagnostic}
	code, err := r.run(ctx, remaining)
	if err != nil {
		_, _ = fmt.Fprintln(diagnostic, err.Error())
		if code == 0 {
			code = _exitFailure
		}
	}
	if ctx.Err() != nil && code != 0 {
		return 130
	}
	return code
}

func (r *runner) run(ctx context.Context, args []string) (int, error) {
	switch args[0] {
	case "profiles":
		return r.profiles(ctx, args[1:])
	case "list":
		return r.list(ctx, args[1:])
	case "read":
		if len(args) != 2 {
			return _exitUsage, errors.New("read requires one conversation ID or URL")
		}
		path, err := r.conversationPath(args[1])
		if err != nil {
			return _exitUsage, err
		}
		var detail any
		if err := r.client.request(ctx, http.MethodGet, path, "", nil, &detail); err != nil {
			return _exitFailure, err
		}
		return 0, r.emit(detail)
	case "watch":
		return r.watch(ctx, args[1:])
	case _commandStart, "continue", _commandClose, _commandFresh:
		return r.mutate(ctx, args[0], args[1:])
	case "resume":
		return r.resume(ctx, args[1:])
	case "operations":
		if len(args) != 1 {
			return _exitUsage, errors.New("operations takes no arguments")
		}
		j, err := openJournal(r.stateDir)
		if err != nil {
			return _exitFailure, err
		}
		defer j.close()
		ids, err := j.list()
		if err != nil {
			return _exitFailure, err
		}
		return 0, r.emit(map[string]any{"operations": ids})
	default:
		return _exitUsage, errors.New("unknown command; use --help")
	}
}

func commandFlags(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func (r *runner) profiles(ctx context.Context, args []string) (int, error) {
	if len(args) > 0 && args[0] == "show" {
		if len(args) != 2 || !validProfileIdentifier(args[1]) {
			return _exitUsage, errors.New("profiles show requires one profile name")
		}
		var result map[string]any
		if err := r.client.request(ctx, http.MethodGet, r.client.basePath()+"/profiles/"+args[1], "", nil, &result); err != nil {
			return _exitFailure, err
		}
		profile, ok := result["profile"].(map[string]any)
		if !ok || profile[_namespaceField] != r.client.namespace {
			return _exitFailure, errors.New("profile response does not match the selected namespace")
		}
		return 0, r.emit(result)
	}
	if len(args) > 0 && args[0] == "list" {
		args = args[1:]
	}
	flags := commandFlags("profiles list")
	limit := flags.Int("limit", 100, "page size")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *limit < 1 || *limit > 100 {
		return _exitUsage, errors.New("profiles list accepts --limit from 1 to 100")
	}
	all := make([]map[string]any, 0)
	after, source, subject := "", "", ""
	seen := make(map[string]bool)
	for page := range 10000 {
		path := r.client.basePath() + "/profiles?limit=" + fmt.Sprint(*limit)
		if after != "" {
			path += "&after=" + url.QueryEscape(after)
		}
		var result struct {
			Profiles []map[string]any `json:"profiles"`
			More     bool             `json:"more"`
			Next     string           `json:"next_cursor"`
			Source   string           `json:"source"`
			Subject  string           `json:"caller_subject"`
		}
		if err := r.client.request(ctx, http.MethodGet, path, "", nil, &result); err != nil {
			return _exitFailure, err
		}
		if page > 0 && (source != result.Source || subject != result.Subject) {
			return _exitFailure, errors.New("profile catalog identity changed during pagination; retry the read")
		}
		source, subject = result.Source, result.Subject
		for _, profile := range result.Profiles {
			if profile[_namespaceField] != r.client.namespace {
				return _exitFailure, errors.New("profile response does not match the selected namespace")
			}
			all = append(all, profile)
		}
		if !result.More {
			return 0, r.emit(map[string]any{"profiles": all, "source": source, _callerSubjectField: subject})
		}
		if result.Next == "" || seen[result.Next] || len(result.Profiles) == 0 {
			return _exitFailure, errors.New("invalid profile pagination; catalog is incomplete")
		}
		seen[result.Next] = true
		after = result.Next
	}
	return _exitFailure, errors.New("profile pagination limit reached; catalog is incomplete")
}

func (r *runner) list(ctx context.Context, args []string) (int, error) {
	flags := commandFlags("list")
	limit := flags.Int("limit", 100, "page size")
	after := flags.String("after", "", "next_cursor from a prior listing")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *limit < 1 || *limit > 100 {
		return _exitUsage, errors.New("list accepts --limit from 1 to 100 and --after cursor")
	}
	path := r.client.basePath() + "/conversations?limit=" + fmt.Sprint(*limit)
	if *after != "" {
		path += "&after=" + url.QueryEscape(*after)
	}
	var result any
	if err := r.client.request(ctx, http.MethodGet, path, "", nil, &result); err != nil {
		return _exitFailure, err
	}
	return 0, r.emit(result)
}

func (r *runner) mutate(ctx context.Context, command string, args []string) (int, error) {
	op, err := r.mutationRequest(command, args)
	if err != nil {
		return _exitUsage, err
	}
	if op.ID == "" {
		op.ID, err = newID()
		if err != nil {
			return _exitFailure, err
		}
	}
	op.Key = op.ID
	j, err := openJournal(r.stateDir)
	if err != nil {
		return _exitFailure, err
	}
	defer j.close()
	op.Subject, err = r.client.subject(ctx)
	if err != nil {
		return _exitFailure, err
	}
	op.Version, op.Origin, op.Namespace = _journalVersion, r.client.origin, r.client.namespace
	op.CreatedAt, op.Baseline = time.Now().UTC(), make([]rootTurn, 0)
	existing, loadErr := j.load(op.ID)
	if loadErr == nil {
		if !sameRequest(existing, op) {
			return _exitFailure, errors.New("saved operation conflicts with body, target, namespace or service replay subject")
		}
		op = existing
	} else {
		if !errors.Is(loadErr, os.ErrNotExist) {
			return _exitFailure, loadErr
		}
		op, err = r.freezeBaseline(ctx, op)
		if err != nil {
			return _exitFailure, err
		}
		op, err = j.save(op)
		if err != nil {
			return _exitFailure, err
		}
	}
	_, _ = fmt.Fprintln(r.diagnostic, "saved operation", op.ID, "(resume with the same origin, namespace and caller)")
	return r.submit(ctx, j, op)
}

func (r *runner) mutationRequest(command string, args []string) (operation, error) {
	path := r.client.basePath() + "/conversations"
	if command != _commandStart {
		if len(args) == 0 {
			return operation{}, errors.New("mutation requires the original conversation ID or URL")
		}
		var err error
		path, err = r.conversationPath(args[0])
		if err != nil {
			return operation{}, err
		}
		args = args[1:]
	}
	flags := commandFlags(command)
	var id, textFile, profile, title string
	flags.StringVar(&id, "operation", "", "stable saved operation identity")
	if command != _commandClose {
		flags.StringVar(&textFile, "text-file", "", "task text file or '-' for stdin")
	}
	if command == _commandStart {
		flags.StringVar(&profile, "profile", "", "approved profile")
		flags.StringVar(&title, "title", "", "optional title")
	}
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return operation{}, errors.New("invalid mutation flags; use --help")
	}
	if command == _commandStart && !validProfileIdentifier(profile) {
		return operation{}, errors.New("start requires --profile")
	}
	if !utf8.ValidString(title) || len(title) > 256 || strings.ContainsFunc(title, unicode.IsControl) {
		return operation{}, errors.New("title must be valid UTF-8, at most 256 bytes and without control characters")
	}
	text := ""
	if command != _commandClose {
		var err error
		text, err = r.readText(textFile)
		if err != nil {
			return operation{}, err
		}
	}
	var body []byte
	switch command {
	case _commandStart:
		body, _ = json.Marshal(struct {
			RouteID string `json:"route_id"`
			Text    string `json:"text"`
			Title   string `json:"title,omitempty"`
		}{RouteID: profile, Text: text, Title: title})
	case _commandClose:
		body = []byte(`{}`)
	default:
		body, _ = json.Marshal(struct {
			Text string `json:"text"`
		}{Text: text})
	}
	if len(body) > _maxTextBytes+1024 {
		return operation{}, errors.New("encoded input exceeds the gateway request limit")
	}
	if id != "" && !validOperationID(id) {
		return operation{}, errors.New("invalid operation ID")
	}
	op := operation{ID: id, Command: command, Path: path, Body: body}
	if command != _commandStart {
		suffix := map[string]string{"continue": "/messages", _commandClose: "/close", _commandFresh: "/restart"}[command]
		op.Path += suffix
	}
	return op, nil
}

func (r *runner) freezeBaseline(ctx context.Context, op operation) (operation, error) {
	if op.Command == _commandStart {
		op.Attempt = 1
		return op, nil
	}
	path := op.Path[:strings.LastIndex(op.Path, "/")]
	detail, err := r.detail(ctx, path)
	if err != nil {
		return operation{}, err
	}
	op.Attempt = detail.Summary.Attempt
	roots, rootsErr := observedRoots(detail)
	if rootsErr == nil && !detail.Summary.ObservationUnavailable {
		op.Baseline, op.BaselineKnown = roots, detail.History.ID != ""
	}
	if op.Command == _commandFresh && (!detail.Summary.Active || detail.Summary.State != _stateClosed || !detail.Summary.Cleaned) {
		return operation{}, errors.New("start-fresh requires the latest attempt to be closed with verified cleanup")
	}
	return op, nil
}

func (r *runner) resume(ctx context.Context, args []string) (int, error) {
	if len(args) != 1 || !validOperationID(args[0]) {
		return _exitUsage, errors.New("resume requires one saved operation ID")
	}
	j, err := openJournal(r.stateDir)
	if err != nil {
		return _exitFailure, err
	}
	defer j.close()
	op, err := j.load(args[0])
	if err != nil {
		return _exitFailure, errors.New("read saved operation")
	}
	subject, err := r.client.subject(ctx)
	if err != nil {
		return _exitFailure, err
	}
	if op.Origin != r.client.origin || op.Namespace != r.client.namespace || op.Subject != subject {
		return _exitFailure, errors.New("saved operation conflicts with origin, namespace or service replay subject")
	}
	return r.submit(ctx, j, op)
}

func (r *runner) submit(ctx context.Context, j *journal, op operation) (int, error) {
	// A prior process may have exited after publishing but before syncing the
	// directory. Establish durability before replay or success in this process.
	if err := j.sync(); err != nil {
		return _exitFailure, err
	}
	accepted, err := j.loadReceipt(op.ID)
	if err == nil {
		if err := r.validateReceipt(op, accepted); err != nil {
			return _exitAmbiguous, err
		}
		if err := j.sync(); err != nil {
			return _exitAmbiguous, err
		}
		return 0, r.emitReceipt(op, accepted, true)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return _exitFailure, err
	}
	if err := r.client.request(ctx, http.MethodPost, op.Path, op.Key, op.Body, &accepted); err != nil {
		code := _exitAmbiguous
		var response *apiError
		if errors.As(err, &response) && response.status >= 400 && response.status < 500 {
			code = _exitFailure
		}
		return code, fmt.Errorf("operation %s retained: %w", op.ID, err)
	}
	if err := r.validateReceipt(op, accepted); err != nil {
		return _exitAmbiguous, err
	}
	if err := j.saveReceipt(op.ID, accepted); err != nil {
		return _exitAmbiguous, fmt.Errorf("acceptance received but receipt persistence failed; resume operation %s: %w", op.ID, err)
	}
	return 0, r.emitReceipt(op, accepted, false)
}

func (r *runner) validateReceipt(op operation, value receipt) error {
	path, err := r.conversationPath(value.ConversationID)
	if err != nil || path != value.ConversationURL || value.EventID == "" || value.Acceptance != "committed" {
		return errors.New("receipt identity invalid; retain and resume the saved operation")
	}
	expectedAttempt := op.Attempt
	if op.Command == _commandFresh {
		expectedAttempt++
	}
	if value.Attempt != expectedAttempt {
		return errors.New("receipt attempt differs from the saved target; retain the operation")
	}
	if op.Command != _commandStart && op.Command != _commandFresh && !strings.HasPrefix(op.Path, path+"/") {
		return errors.New("receipt target differs from the saved target; retain the operation")
	}
	return nil
}

func (r *runner) emitReceipt(op operation, value receipt, saved bool) error {
	result := map[string]any{_stateField: _stateAccepted, _operationIDField: op.ID, "receipt": value,
		_correlationField: _correlationUnavailable, "saved_receipt": saved}
	if op.Command == _commandFresh {
		result["workspace"] = "new attempt creates a new workspace; any retained old workspace is not adopted"
	}
	return r.emit(result)
}

func (r *runner) readText(path string) (string, error) {
	if path == "" {
		return "", errors.New("--text-file is required; use '-' for stdin")
	}
	input := r.in
	if path != "-" {
		file, err := os.Open(path)
		if err != nil {
			return "", errors.New("read input text file")
		}
		defer func() { _ = file.Close() }()
		input = file
	}
	raw, err := io.ReadAll(io.LimitReader(input, _maxTextBytes+1))
	if err != nil || len(raw) > _maxTextBytes || !utf8.Valid(raw) {
		return "", errors.New("text must be valid UTF-8 and at most 256 KiB")
	}
	text := strings.ReplaceAll(string(raw), "\r\n", "\n")
	if strings.TrimSpace(text) == "" {
		return "", errors.New("text must not be empty")
	}
	return text, nil
}

func (r *runner) conversationPath(value string) (string, error) {
	base := r.client.basePath() + "/conversations/"
	if suffix, found := strings.CutPrefix(value, base); found {
		value = suffix
	}
	if strings.HasPrefix(value, "https://") || strings.HasPrefix(value, "http://") {
		u, err := url.Parse(value)
		if err != nil || u.Scheme+"://"+u.Host != r.client.origin || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return "", errors.New("conversation URL must use the configured origin and namespace")
		}
		value = strings.TrimPrefix(u.Path, base)
	}
	if !strings.HasPrefix(value, "conv-") || !validIdentifier(value) {
		return "", errors.New("conversation ID or original URL required")
	}
	return base + value, nil
}

func (r *runner) detail(ctx context.Context, path string) (conversationDetail, error) {
	var detail conversationDetail
	if err := r.client.request(ctx, http.MethodGet, path, "", nil, &detail); err != nil {
		return detail, err
	}
	if detail.Summary.ConversationURL != path || detail.Summary.Attempt < 1 {
		return detail, errors.New("conversation response differs from the selected target")
	}
	return detail, nil
}

func validIdentifier(value string) bool {
	if len(value) < 1 || len(value) > 256 {
		return false
	}
	for _, char := range value {
		letter := char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z'
		digit := char >= '0' && char <= '9'
		if !letter && !digit && char != '_' && char != '-' && char != '.' {
			return false
		}
	}
	return value != "." && value != ".."
}

func validProfileIdentifier(value string) bool {
	return len(validation.IsDNS1123Subdomain(value)) == 0
}

func (r *runner) emit(value any) error {
	if err := json.NewEncoder(r.out).Encode(value); err != nil {
		return errors.New("write output")
	}
	return nil
}
