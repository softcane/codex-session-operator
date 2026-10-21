package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
)

const (
	_testApprover  = "approver"
	_testExternal  = "external"
	_testOriginal  = "original"
	_testUncertain = "uncertain"
)

func postgresForTest(t testing.TB) (*PostgreSQLStore, string) {
	t.Helper()
	return postgresForTestLimits(t, 32, 100000)
}

func postgresForTestLimits(t testing.TB, active, retained int) (*PostgreSQLStore, string) {
	t.Helper()
	dsn := postgresDatabaseForTest(t)
	options := PostgreSQLConfig{DSN: dsn, MaxActive: active, MaxRecords: retained, AllowInsecureLocal: true}
	store, err := OpenPostgreSQL(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, dsn
}

func postgresDatabaseForTest(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv("GATEWAY_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("requires real PostgreSQL; run make test-storage-postgres")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal("connect test database", safeSQLError(err))
	}
	database := "gateway_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{database}.Sanitize()); err != nil {
		t.Fatal(safeSQLError(err))
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("parse test database")
	}
	// The test harness supplies a local URL with no credentials.
	dsn = strings.Replace(dsn, "/postgres?", "/"+database+"?", 1)
	if config.Database != "postgres" {
		t.Fatal("test DSN must name postgres database")
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)")
		_ = admin.Close(ctx)
	})
	return dsn
}

func TestPostgreSQLStorageBehavior(t *testing.T) {
	store, _ := postgresForTest(t)
	t.Run("atomic mutation and caller ownership", func(t *testing.T) {
		if err := store.Update(t.Context(), "contract", func(r *Record) error { r.Namespace = _testNamespace; r.Notes["a"] = _testOriginal; return nil }); err != nil {
			t.Fatal(err)
		}
		r, _ := store.Get(context.Background(), "contract")
		r.Notes["a"] = "mutated"
		if err := store.Update(t.Context(), "contract", func(r *Record) error { r.Notes["a"] = "partial"; return errConflict }); !errors.Is(err, errConflict) {
			t.Fatal(err)
		}
		r, _ = store.Get(context.Background(), "contract")
		if r.Notes["a"] != _testOriginal {
			t.Fatal("partial mutation escaped")
		}
	})
	t.Run("namespace binding and isolation", func(t *testing.T) { checkNamespaceBinding(t, store) })
	t.Run("receipt replay and changed input", func(t *testing.T) {
		g, _, _ := profileConversationGateway(t, store)
		body := _profileTestBody
		first := request(g, http.MethodPost, _conversationPath, "backend-event", body)
		if first.Code != 202 {
			t.Fatal(first.Code, first.Body.String())
		}
		second := request(g, http.MethodPost, _conversationPath, "backend-event", body)
		if second.Code != 202 || second.Body.String() != first.Body.String() {
			t.Fatal("receipt changed")
		}
		changed := strings.Replace(body, "Inspect the repository", "Changed", 1)
		if response := request(g, http.MethodPost, _conversationPath, "backend-event", changed); response.Code != 409 {
			t.Fatal(response.Code)
		}
	})
	t.Run("lost provider reply and pending approval", func(t *testing.T) {
		p := simulatorForTest()
		g, _ := testGateway(t, store, p, testSession())
		if response := request(g, http.MethodPost, _testMessagePath, "contract-input", `{"text":"queued"}`); response.Code != 202 {
			t.Fatal(response.Code)
		}
		p.lose = true
		if err := g.sync(context.Background(), _testUID); err == nil {
			t.Fatal("lost provider reply hidden")
		}
		before := mustRecord(t, store).Submissions[0].RemoteKey
		if err := g.sync(context.Background(), _testUID); err != nil {
			t.Fatal(err)
		}
		if p.commits != 1 || mustRecord(t, store).Submissions[0].RemoteKey != before {
			t.Fatal("duplicate logical delivery")
		}
		p.history.Actions = []openaiapi.PendingAction{{Type: _functionCall, TurnID: _testTurnID, CallID: _testCallID, Name: _noteFunction, Arguments: json.RawMessage(`{"note":"approved"}`)}}
		if err := g.sync(context.Background(), _testUID); err != nil {
			t.Fatal(err)
		}
		if err := g.decide(t.Context(), _testUID, actionID(p.history.Actions[0]), _approve, "original-approver"); err != nil {
			t.Fatal(err)
		}
		g2, _ := testGateway(t, store, p, testSession())
		if err := g2.sync(context.Background(), _testUID); err != nil {
			t.Fatal(err)
		}
		r := mustRecord(t, store)
		a := r.Actions[actionID(p.history.Actions[0])]
		if a.DecidedBy != "original-approver" || a.SubmissionKey != resultKey(_testUID, actionID(p.history.Actions[0])) || len(r.Notes) != 1 {
			t.Fatal("approval identity changed")
		}
	})
	t.Run("guarded purge", func(t *testing.T) {
		if err := store.Purge(t.Context(), "contract"); err == nil {
			t.Fatal("active purge succeeded")
		}
		if err := store.Update(t.Context(), "contract", func(r *Record) error { r.Closed = true; return nil }); err != nil {
			t.Fatal(err)
		}
		if err := store.Purge(t.Context(), "contract"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Get(t.Context(), "contract"); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	})
}

func TestPostgreSQLConcurrentAcceptance(t *testing.T) {
	store, dsn := postgresForTest(t)
	other, err := OpenPostgreSQL(context.Background(), PostgreSQLConfig{DSN: dsn, AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	g1, _, _ := profileConversationGateway(t, store)
	g2, _, _ := profileConversationGateway(t, other)
	body := _profileTestBody
	responses := make(chan string, 20)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			g := g1
			if i%2 == 1 {
				g = g2
			}
			r := request(g, http.MethodPost, _conversationPath, "concurrent-delivery", body)
			responses <- fmt.Sprintf("%d:%s", r.Code, r.Body.String())
		})
	}
	wg.Wait()
	close(responses)
	first := ""
	for response := range responses {
		if !strings.HasPrefix(response, "202:") {
			t.Fatal(response)
		}
		if first == "" {
			first = response
		}
		if response != first {
			t.Fatal("concurrent receipt changed")
		}
	}
	records, err := store.Records(context.Background())
	if err != nil || len(records) != 1 || len(records[0].Work.Events) != 1 {
		t.Fatal("duplicate acceptance", err)
	}
	changed := strings.Replace(body, "Inspect the repository", "different", 1)
	if r := request(g2, http.MethodPost, _conversationPath, "concurrent-delivery", changed); r.Code != 409 {
		t.Fatal("creation key accepted changed input", r.Code)
	}
}

func TestPostgreSQLArchiveErasesPayloadsAndPreservesReplay(t *testing.T) {
	store, _ := postgresForTest(t)
	const privateText = "synthetic-private-archive-content"
	session := testSession()
	g, _ := testGateway(t, store, simulatorForTest(), session)
	if err := g.enqueue(t.Context(), session, "archive-payload", privateText, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), _testUID, func(r *Record) error {
		r.Closed = true
		r.History.Items = []json.RawMessage{inputEvent(privateText)}
		r.Notes["private"] = privateText
		appendEvent(r, inputEvent(privateText))
		r.Actions["action"] = Action{
			Pending: openaiapi.PendingAction{Type: _functionCall, CallID: "call", Arguments: mustJSON(t, map[string]string{"text": privateText}), Request: inputEvent(privateText)},
			Result:  inputEvent(privateText), OperationID: "operation", Decision: _approve, Active: true,
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := store.Archive(t.Context(), _testUID); err != nil {
			t.Fatal(err)
		}
	}
	archived, err := store.Get(t.Context(), _testUID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mustJSON(t, archived)), privateText) {
		t.Fatal("archive retained conversation payloads")
	}
	if archived.Actions["action"].OperationID != "operation" || archived.Actions["action"].Decision != _approve || archived.Submissions[0].ReplayDigest == "" {
		t.Fatal("archive lost immutable action or replay identity")
	}
	backup, err := store.Backup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := postgresForTest(t)
	if _, err := fresh.Restore(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	for _, backend := range []*PostgreSQLStore{store, fresh} {
		g.store = backend
		if err := g.enqueue(t.Context(), session, "archive-payload", privateText, "", nil); err != nil {
			t.Fatalf("exact archived replay failed: %v", err)
		}
		if err := g.enqueue(t.Context(), session, "archive-payload", "changed", "", nil); !errors.Is(err, errConflict) {
			t.Fatalf("changed archived replay accepted: %v", err)
		}
	}
}

type heldReceiptWriter struct {
	http.ResponseWriter
	path   string
	status int
}

func (w *heldReceiptWriter) WriteHeader(status int) { w.status = status }

func (w *heldReceiptWriter) Write(raw []byte) (int, error) {
	if w.status != http.StatusAccepted {
		return 0, errors.New("crash test acceptance rejected")
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return 0, err
	}
	_, writeErr := file.Write(raw)
	syncErr := file.Sync()
	closeErr := file.Close()
	if errors.Join(writeErr, syncErr, closeErr) != nil {
		return 0, errors.New("crash receipt evidence not durable")
	}
	fmt.Println("COMMITTED_RECEIPT_HELD")
	// Intentional integration fault: only SIGKILL releases this blocked response.
	select {}
}

func TestPostgreSQLBackupRestore(t *testing.T) {
	store, _ := postgresForTest(t)
	g, _, _ := profileConversationGateway(t, store)
	response := request(g, http.MethodPost, _conversationPath, "restore-pending", _profileTestBody)
	if response.Code != 202 {
		t.Fatal(response.Code)
	}
	if err := store.Update(t.Context(), _testUncertain, func(r *Record) error {
		r.Namespace = _testNamespace
		r.RemoteID = "original-remote"
		r.Submissions = []Submission{{Key: _testUncertain, RemoteKey: wireKey(_testUncertain, _testUncertain), Attempts: 1, Text: "uncertain input"}}
		r.Actions["call"] = Action{Decision: _approve, DecidedBy: "alice", OperationID: "original-operation", AttemptedAt: new(time.Now())}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	seedCompletedRecord(t, store)
	original, err := store.Records(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	backup, err := store.Backup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// The backup predates a remote outcome marker; restoring it must stay gated.
	if err := store.Update(t.Context(), _testUncertain, func(r *Record) error { r.Submissions[0].Accepted = true; return nil }); err != nil {
		t.Fatal(err)
	}
	fresh, _ := postgresForTest(t)
	started := time.Now()
	report, err := fresh.Restore(context.Background(), backup)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := fresh.Records(context.Background())
	if err != nil || !reflect.DeepEqual(original, restored) {
		t.Fatal("restore changed saved identities", err)
	}
	t.Logf("restoration=%s backup_age=%s deliberately_missing_post_backup_markers=1", time.Since(started), time.Since(backup.CreatedAt))
	if !errors.Is(fresh.DeliveryAllowed(t.Context()), errRestoreHeld) {
		t.Fatal("restore authorized remote delivery")
	}
	if _, _, err := fresh.ClaimWorker(context.Background()); err == nil {
		t.Fatal("restore acquired worker")
	}
	assertRestoredRPCGate(t, fresh)
	if err := fresh.ResumeDelivery(t.Context(), nil); err == nil {
		t.Fatal("unreviewed restore enabled")
	}
	audit := make([]Reconciliation, 0, len(report.Digests))
	for uid, digest := range report.Digests {
		audit = append(audit, Reconciliation{UID: uid, Digest: digest,
			Reference: "test exact Kubernetes/provider audit", Blocked: uid == _testUncertain, Verified: uid != _testUncertain})
	}
	if err := fresh.ResumeDelivery(t.Context(), audit); err != nil {
		t.Fatal(err)
	}
	uncertain, err := fresh.Get(context.Background(), _testUncertain)
	if err != nil || !uncertain.DeliveryHeld || uncertain.Submissions[0].RemoteKey != wireKey(_testUncertain, _testUncertain) || uncertain.Actions["call"].OperationID != "original-operation" {
		t.Fatal("restore lost unresolved claims", err)
	}
	if _, err := fresh.Restore(t.Context(), backup); err == nil {
		t.Fatal("restore overwrote live database")
	}
	if err := store.Update(t.Context(), _testUncertain, func(r *Record) error { r.Submissions[0].Accepted = true; return nil }); err != nil {
		t.Fatal(err)
	}

	uncertain, _ = store.Get(context.Background(), _testUncertain)
	if !uncertain.Submissions[0].Accepted {
		t.Fatal("database update reverted acceptance")
	}
}

type commitDisconnect struct {
	net.Conn
	armed *atomic.Bool
	drop  bool
}

// The harness runs seed and verify in separate Go processes around SIGKILL of
// the real database. The seed receipt is withheld from the simulated caller.
func TestPostgreSQLCrashProof(t *testing.T) {
	stage := os.Getenv("GATEWAY_TEST_CRASH_STAGE")
	if stage == "" {
		t.Skip("requires database SIGKILL harness")
	}
	options := PostgreSQLConfig{
		DSN: os.Getenv("GATEWAY_TEST_POSTGRES"), AllowInsecureLocal: true,
	}
	store, err := OpenPostgreSQL(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	g, _, _ := profileConversationGateway(t, store)
	if stage == "serve" {
		// The integration harness kills this process after the database commit,
		// while the real HTTP connection has received neither headers nor body.
		server := &http.Server{Addr: ":8090", ReadHeaderTimeout: time.Second,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				g.ServeHTTP(&heldReceiptWriter{ResponseWriter: w,
					path: os.Getenv("GATEWAY_TEST_CRASH_RECEIPT_FILE")}, r)
			})}
		t.Log("CRASH_HTTP_LISTENER_READY")
		t.Fatal(server.ListenAndServe())
	}
	body := `{"route_id":"payments","text":"durably accepted"}`
	receipt := request(g, http.MethodPost, _conversationPath, "crash-proof", body)
	if receipt.Code != http.StatusAccepted {
		t.Fatal(receipt.Code)
	}
	path := os.Getenv("GATEWAY_TEST_CRASH_RECEIPT_FILE")
	if stage == "seed" {
		if err := os.WriteFile(path, receipt.Body.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	original, err := os.ReadFile(path)
	if err != nil || string(original) != receipt.Body.String() {
		t.Fatal("process crash lost original receipt", err)
	}
	changed := strings.Replace(body, "durably accepted", "different", 1)
	if response := request(g, http.MethodPost, _conversationPath, "crash-proof", changed); response.Code != http.StatusConflict {
		t.Fatal("process crash lost replay conflict", response.Code)
	}
}

func (c *commitDisconnect) Write(raw []byte) (int, error) {
	if len(raw) > 5 && raw[0] == 'Q' && strings.EqualFold(string(raw[5:]), "commit\x00") && c.armed.CompareAndSwap(true, false) {
		c.drop = true
	}
	return c.Conn.Write(raw)
}
func (c *commitDisconnect) Read(raw []byte) (int, error) {
	n, err := c.Conn.Read(raw)
	if c.drop && n > 0 {
		_ = c.Close()
		return 0, io.ErrUnexpectedEOF
	}
	return n, err
}

func TestPostgreSQLUnknownCommitAndDisconnect(t *testing.T) {
	store, _ := postgresForTest(t)
	armed := new(atomic.Bool)
	config := store.pool.Config()
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &commitDisconnect{Conn: conn, armed: armed}, nil
	}
	// Reopen with a transport that drops the actual server COMMIT response.
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	proxy := &PostgreSQLStore{pool: pool}
	armed.Store(true)
	if err := proxy.Update(t.Context(), "lost-commit", func(r *Record) error { r.Namespace = _testNamespace; return nil }); err != nil {
		r, getErr := store.Get(t.Context(), "lost-commit")
		t.Fatalf("commit result=%v faultArmed=%v persistedUID=%s lookup=%v", err, armed.Load(), r.UID, getErr)
	}
	if armed.Load() {
		t.Fatal("commit fault did not fire")
	}
	if _, err := store.Get(t.Context(), "lost-commit"); err != nil {
		t.Fatal(err)
	}
	err = proxy.Update(context.Background(), "before-commit", func(r *Record) error {
		_, killErr := store.pool.Exec(context.Background(), "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='idle in transaction'")
		if killErr != nil {
			return errUnavailable
		}
		r.Namespace = _testNamespace
		return nil
	})
	if !errors.Is(err, errUnavailable) {
		t.Fatal("disconnect falsely accepted", err)
	}
	if _, err := store.Get(t.Context(), "before-commit"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("disconnected transaction persisted", err)
	}
}

func TestPostgreSQLWorkerFencingAndOperationRecovery(t *testing.T) {
	store, _ := postgresForTest(t)
	first, release, err := store.ClaimWorker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, _, err := store.ClaimWorker(context.Background()); !errors.Is(err, errWorkerHeld) {
		t.Fatal("two workers acquired ownership", err)
	}
	p := simulatorForTest()
	p.history.Actions = []openaiapi.PendingAction{{Type: _functionCall, TurnID: _testTurnID, CallID: _testCallID, Name: _testExternal, Arguments: json.RawMessage(`{"value":1}`)}}
	g, _ := testGateway(t, first, p, testSession())
	_ = request(g, http.MethodGet, "/v1/namespaces/team/sessions/work", "", "")
	if err := g.recover(t.Context(), _testUID, p.history, false); err != nil {
		t.Fatal(err)
	}
	if err := g.decide(t.Context(), _testUID, actionID(p.history.Actions[0]), _approve, _testApprover); err != nil {
		t.Fatal(err)
	}
	ids := make(map[string]bool)
	g.functions[_testExternal] = Function{Consequential: true, DownstreamDeduplicates: true, Execute: func(_ context.Context, op Operation) (string, error) {
		stored := mustRecord(t, store).Actions[actionID(p.history.Actions[0])]
		if stored.OperationID != op.ID || stored.AttemptedAt == nil {
			t.Fatal("external side effect preceded durable claim")
		}
		ids[op.ID] = true
		_, err := store.pool.Exec(context.Background(), "UPDATE gateway_meta SET expires='-infinity' WHERE id=1")
		return "committed downstream", err
	}}
	if err := g.processActions(context.Background(), _testUID, p.history); !errors.Is(err, errFenced) {
		t.Fatal("stale worker committed result", err)
	}
	second, release2, err := store.ClaimWorker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release2()
	if err := first.Update(t.Context(), _testUID, func(r *Record) error { r.Closed = true; return nil }); !errors.Is(err, errFenced) {
		t.Fatal("old epoch mutated state", err)
	}
	g2, _ := testGateway(t, second, p, testSession())
	g2.functions[_testExternal] = Function{Consequential: true, DownstreamDeduplicates: true, Execute: func(_ context.Context, op Operation) (string, error) {
		ids[op.ID] = true
		return "recovered downstream", nil
	}}
	if err := g2.processActions(context.Background(), _testUID, p.history); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatal("replacement changed operation ID")
	}
	a := mustRecord(t, store).Actions[actionID(p.history.Actions[0])]
	if a.DecidedBy != _testApprover || a.SubmissionKey != a.OperationID {
		t.Fatal("replacement lost approval/operation binding")
	}
}

func assertRestoredRPCGate(t *testing.T, store Storage) {
	t.Helper()
	heldGateway, _ := testGateway(t, store, simulatorForTest(), testSession())
	if _, _, err := heldGateway.InitialInput(context.Background(), _testUID); !errors.Is(err, errRestoreHeld) {
		t.Fatal("restored initial input was not gated", err)
	}
	if err := heldGateway.Quiesce(context.Background(), _testUID, _testRemoteID); !errors.Is(err, errRestoreHeld) {
		t.Fatal("restored cancellation was not gated", err)
	}
}

func checkNamespaceBinding(t *testing.T, store Storage) {
	t.Helper()
	if err := store.Update(t.Context(), "namespace-contract", func(*Record) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(t.Context(), "namespace-contract", func(r *Record) error { r.Namespace = _testNamespace; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetInNamespace(t.Context(), _testNamespace, "namespace-contract"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetInNamespace(t.Context(), "foreign-tenant", "namespace-contract"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cross namespace read", err)
	}
	records, err := store.RecordsInNamespace(context.Background(), "foreign-tenant")
	if err != nil || len(records) != 0 {
		t.Fatal("cross namespace listing", err)
	}
}

func TestPostgreSQLLeaseExpiryDuringMutation(t *testing.T) {
	store, _ := postgresForTest(t)
	if err := store.Update(t.Context(), "expiry", func(r *Record) error { r.Namespace = _testNamespace; return nil }); err != nil {
		t.Fatal(err)
	}
	view, release, err := store.ClaimWorker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := store.pool.Exec(context.Background(), "UPDATE gateway_meta SET expires=clock_timestamp()+interval '300 milliseconds' WHERE id=1"); err != nil {
		t.Fatal("set lease expiry")
	}
	if err := view.Update(t.Context(), "expiry", func(r *Record) error { time.Sleep(400 * time.Millisecond); r.Closed = true; return nil }); !errors.Is(err, errFenced) {
		t.Fatal("expired lease committed", err)
	}
	record, err := store.Get(context.Background(), "expiry")
	if err != nil || record.Closed {
		t.Fatal("expired mutation became visible", err)
	}
}

func TestPostgreSQLRetainedQuotaReservesConversation(t *testing.T) {
	store, _ := postgresForTest(t)
	if _, err := store.pool.Exec(context.Background(), "UPDATE gateway_meta SET max_records=1 WHERE id=1"); err != nil {
		t.Fatal("configure retained quota")
	}
	g, _, _ := profileConversationGateway(t, store)
	response := request(g, http.MethodPost, _conversationPath, "no-room-for-conversation", _profileTestBody)
	if response.Code == 202 {
		t.Fatal("accepted work without its future conversation capacity")
	}
	records, err := store.Records(context.Background())
	if err != nil || len(records) != 0 {
		t.Fatal("rejected reservation persisted", err)
	}
}

func seedCompletedRecord(t *testing.T, store Storage) {
	t.Helper()
	if err := store.Update(t.Context(), "completed", func(r *Record) error {
		r.Namespace = _testNamespace
		r.Closed = true
		r.RemoteID = "completed-remote"
		r.Submissions = []Submission{{Key: "completed-input", RemoteKey: wireKey("completed", "completed-input"), Accepted: true, Text: "completed input"}}
		r.Actions["completed-call"] = Action{Decision: _approve, DecidedBy: "alice", OperationID: "completed-operation", Result: json.RawMessage(`{"type":"agent.session.input.tool_result","success":true}`)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
