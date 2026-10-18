package openaiapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
)

const testRemoteURL = "https://api.openai.com/v1/agents/api"
const testSessionsPath = "/v1/agents/sessions"

const testEnvironmentID = "env_test"
const testSessionID = "sess_test"
const paginatedScenario = "paginated"
const createFailureScenario = "create-500"

const deleteMissingScenario = "delete-404"

const deleteConfirmedScenario = "delete-confirmed"

const getMissingScenario = "get-404"

func remoteJSON(uid, id string) map[string]any {
	return map[string]any{"id": id, objectField: "agent.session", "status": "idle", agentField: map[string]any{modelField: SessionModel}, metadataField: map[string]string{ownerKey: uid, operatorKey: operatorValue}, testEnvironmentField: map[string]any{typeField: selfHostedType, "id": testEnvironmentID, "remote_url": testRemoteURL, workspaceDirectoryField: testWorkspace}}
}
func clientForServer(server *httptest.Server, transport http.RoundTripper) *Client {
	return &Client{SDK: openai.NewClient(option.WithAPIKey("test-placeholder"), option.WithBaseURL(server.URL+"/v1/"), option.WithMaxRetries(0), option.WithHTTPClient(&http.Client{Transport: transport}))}
}

func createTestConfiguredSession(ctx context.Context, client *Client) (Session, error) {
	spend := &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeUnlimited}
	return client.CreateConfiguredSession(ctx, "uid-test", "test-scope", "",
		map[string]any{typeField: selfHostedType, workspaceDirectoryField: testWorkspace}, nil, nil, nil, spend)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSpendControlUnavailableErrorIsClassifiedWithoutRawBody(t *testing.T) {
	rejected := &openai.Error{StatusCode: http.StatusBadRequest, Param: spendControlField,
		Message: "Session budget configuration is not enabled"}
	if got := safeError(createOperation, rejected); !errors.Is(got, ErrSpendControlUnavailable) {
		t.Fatalf("known project capability rejection was not classified: %v", got)
	}
	other := &openai.Error{StatusCode: http.StatusBadRequest, Param: spendControlField,
		Message: "sensitive unexpected provider text"}
	if got := safeError(createOperation, other); strings.Contains(got.Error(), other.Message) {
		t.Fatal("unrecognized provider message leaked")
	}
}

func TestCreateContractAndLostResponse(t *testing.T) {
	creates := 0
	committed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("OpenAI-Beta") != agentsBetaHeader {
			t.Error("Missing beta header")
		}
		if r.Method == http.MethodPost {
			creates++
			var body struct {
				Agent       struct{ Model string }
				Environment struct {
					Type      string
					Workspace string `json:"workspace_directory"`
				}
				Metadata map[string]string
				Input    json.RawMessage
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.Agent.Model != SessionModel || body.Environment.Type != selfHostedType || body.Environment.Workspace != testWorkspace || body.Metadata[ownerKey] != "uid-test" || len(body.Input) != 0 {
				t.Error("Create request contract mismatch")
			}
			committed = true
			_ = json.NewEncoder(w).Encode(remoteJSON("uid-test", testSessionID))
			return
		}
		if r.Method == http.MethodGet && committed {
			_ = json.NewEncoder(w).Encode(map[string]any{dataField: []any{remoteJSON("uid-test", testSessionID)}, hasMoreField: false})
			return
		}
		t.Error("Unexpected request")
	}))
	defer server.Close()
	drop := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		res, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		if r.Method == http.MethodPost {
			_, _ = io.Copy(io.Discard, res.Body)
			_ = res.Body.Close()
			return nil, errors.New("lost-response-private-detail")
		}
		return res, nil
	})
	c := clientForServer(server, drop)
	_, err := createTestConfiguredSession(context.Background(), c)
	if err == nil || strings.Contains(err.Error(), "private-detail") {
		t.Fatal("Lost response must produce a sanitized error")
	}
	recovered, err := c.FindSession(context.Background(), "uid-test")
	if err != nil || recovered.ID != testSessionID || creates != 1 {
		t.Fatalf("Recovery failed: id=%s creates=%d err=%v", recovered.ID, creates, err)
	}
}

func TestDiscoveryPaginationAndSafety(t *testing.T) {
	for _, scenario := range []string{paginatedScenario, duplicateScenario, limitScenario, unownedScenario, missingPaginationScenario} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				calls++
				page := map[string]any{dataField: []any{remoteJSON("someone-else", "sess_other")}, hasMoreField: false}
				switch scenario {
				case paginatedScenario:
					if calls == 1 {
						page[hasMoreField] = true
					} else {
						if r.URL.Query().Get("after") != "sess_other" {
							t.Error("Missing pagination cursor")
						}
						page[dataField] = []any{remoteJSON("uid-test", "sess_owned")}
					}
				case duplicateScenario:
					page[dataField] = []any{remoteJSON("uid-test", "sess_a"), remoteJSON("uid-test", "sess_b")}
				case limitScenario:
					page[hasMoreField] = true
				case missingPaginationScenario:
					delete(page, hasMoreField)
				}
				_ = json.NewEncoder(w).Encode(page)
			}))
			defer server.Close()
			remote, err := clientForServer(server, nil).FindSession(context.Background(), "uid-test")
			switch scenario {
			case paginatedScenario:
				if err != nil || remote.ID != "sess_owned" || calls != 2 {
					t.Fatalf("Failed pagination: calls=%d err=%v", calls, err)
				}
			case unownedScenario:
				if err != nil || remote.ID != "" {
					t.Fatal("Adopted unowned Session")
				}
			default:
				if err == nil || remote.ID != "" {
					t.Fatal("Unsafe discovery returned a match")
				}
			}
			if calls > maxDiscoveryPages {
				t.Fatal("Unbounded discovery")
			}
		})
	}
}

func TestProviderErrorsAndDeleteConfirmation(t *testing.T) {
	for _, scenario := range []string{getMissingScenario, deleteMissingScenario, "delete-409", "delete-500", deleteUnconfirmedScenario, deleteConfirmedScenario, createFailureScenario} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				calls++
				switch scenario {
				case getMissingScenario, deleteMissingScenario:
					w.WriteHeader(404)
				case "delete-409":
					w.WriteHeader(409)
				case "delete-500", createFailureScenario:
					w.WriteHeader(500)
				case deleteUnconfirmedScenario:
					_ = json.NewEncoder(w).Encode(map[string]any{"id": testSessionID, "deleted": false})
					return
				case deleteConfirmedScenario:
					_ = json.NewEncoder(w).Encode(map[string]any{"id": testSessionID, "deleted": true})
					return
				}
				_, _ = io.WriteString(w, `{"error":{"message":"PRIVATE_RESPONSE test-placeholder","type":"api_error"}}`)
			}))
			defer server.Close()
			c := clientForServer(server, nil)
			var err error
			switch scenario {
			case getMissingScenario:
				_, err = c.GetSession(context.Background(), testSessionID)
				if !errors.Is(err, ErrNotFound) {
					t.Fatal("Missing NotFound classification")
				}
			case createFailureScenario:
				_, err = createTestConfiguredSession(context.Background(), c)
			default:
				err = c.DeleteSession(context.Background(), testSessionID)
			}
			expectSuccess := scenario == deleteMissingScenario || scenario == deleteConfirmedScenario
			if (err == nil) != expectSuccess {
				t.Fatalf("Unexpected success/error: %v", err)
			}
			if err != nil && (strings.Contains(err.Error(), "PRIVATE_RESPONSE") || strings.Contains(err.Error(), "test-placeholder")) {
				t.Fatal("Provider error leaked response details")
			}
			if calls != 1 {
				t.Fatalf("Unexpected automatic retries: %d", calls)
			}
		})
	}
}

func TestContextCancellationAndCallDeadline(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "bounded", true: "canceled"}[canceled], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if canceled {
				cancel()
			}
			transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if _, ok := r.Context().Deadline(); !ok {
					t.Error("External call has no deadline")
				}
				if canceled {
					return nil, r.Context().Err()
				}
				return nil, errors.New("test transport error")
			})
			c := &Client{SDK: openai.NewClient(option.WithAPIKey("test-placeholder"), option.WithHTTPClient(&http.Client{Transport: transport}))}
			_, err := createTestConfiguredSession(ctx, c)
			if err == nil || (canceled && !errors.Is(err, context.Canceled)) {
				t.Fatalf("Context error mismatch: %v", err)
			}
		})
	}
}

func TestConnectionURLRejectsCredentials(t *testing.T) {
	for _, remoteURL := range []string{testRemoteURL, "https://user:secret@example.com/connect", "https://example.com/connect?token=secret", "http://example.com/connect", "https://example.com/connect#secret", ""} {
		remote := Session{ID: testSessionID, EnvironmentID: testEnvironmentID, RemoteURL: remoteURL, SelfHosted: true}
		if (remote.ValidateConnection() == nil) != (remoteURL == testRemoteURL) {
			t.Error("Connection URL validation mismatch")
		}
	}
}
