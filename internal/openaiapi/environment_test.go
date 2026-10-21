package openaiapi

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const testCwdField = "cwd"

const (
	testConnectionOriginField = "connection_origin"
	testCommandField          = "command"
	testEnvironmentField      = "environment"
	testPluginKey             = "plugin"
	testPinnedCapabilityPath  = "/capabilities/helper"
	testServiceOrigin         = "service"
)

const (
	testSmallContainerSize = "small"
)

const (
	testFileKey          = "file"
	testHostedTemplateID = "tmpl_test"
	testSkillKey         = "skill"
)

func TestHostedCompleteWireAndSDKSizeGap(t *testing.T) {
	hosted := &agentsv1alpha1.HostedEnvironment{ContainerSize: testSmallContainerSize, Network: agentsv1alpha1.HostedNetwork{Access: restrictedNetworkAccess, BlockedDomains: []string{"example.com"}},
		Env: []agentsv1alpha1.HostedVariable{{Name: "TEST_VALUE", SecretKey: "env"}}, Files: []agentsv1alpha1.HostedFile{{Type: agentsv1alpha1.HostedFileTypeInline, Path: "/workspace/input", SecretKey: testFileKey}, {Type: agentsv1alpha1.HostedFileTypeFileID, Path: "/workspace/from-api", FileID: "file_test"}},
		SetupCommands: []agentsv1alpha1.HostedSetupCommand{{SecretKey: "setup"}}, Skills: []agentsv1alpha1.HostedSkill{{Type: agentsv1alpha1.HostedSkillTypeSkillReference, SkillID: "skill_test", Version: "2"}}}
	secret := &corev1.Secret{Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"env": []byte("protected-placeholder"), testFileKey: []byte("file-placeholder"), "setup": []byte("mkdir -p outputs")}}
	fields, err := HostedConfiguration(hosted, secret, "audit name")
	if err != nil {
		t.Fatal(err)
	}
	// This fixture is independent of the translator and catches misspelled/omitted fields.
	const expectedJSON = `{"name":"audit name","capability_directories":[],"desktop":{"enabled":false},"env":{"TEST_VALUE":"protected-placeholder"},"files":[{"type":"inline","path":"/workspace/input","data":"ZmlsZS1wbGFjZWhvbGRlcg=="},{"type":"file_id","path":"/workspace/from-api","file_id":"file_test"}],"network":{"access":"restricted","allowed_domains":[],"blocked_domains":["example.com"]},"packages":{"python":[],"system":[],"npm":[]},"plugins":[],"skills":[{"type":"skill_reference","skill_id":"skill_test","version":"2"}],"setup_commands":[{"command":"mkdir -p outputs","cwd":"/workspace"}]}`
	var expected map[string]any
	if err := json.Unmarshal([]byte(expectedJSON), &expected); err != nil {
		t.Fatal(err)
	}
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if r.Header.Get("OpenAI-Beta") != agentsBetaHeader {
			t.Error("beta contract missing")
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/templates") {
			if !reflect.DeepEqual(body, expected) {
				t.Error("template wire fields differ from independent fixture")
			}
			body["id"] = testHostedTemplateID
			_ = json.NewEncoder(w).Encode(body)
			return
		}
		if r.URL.Path != testSessionsPath {
			t.Error("incorrect Session endpoint")
		}
		environment, ok := body[testEnvironmentField].(map[string]any)
		if !ok {
			t.Error("environment missing")
			return
		}
		if environment["container_size"] != testSmallContainerSize || environment["environment_template_id"] != testHostedTemplateID || environment[typeField] != openaiHostedType {
			t.Error("SDK size gap or hosted mapping lost")
		}
		if body["input"] != nil || body["stream"] != nil || body["agent_id"] != "agent_test" {
			t.Error("Session ownership boundary violated")
		}
		body["id"] = "sess_test"
		body["agent"] = map[string]any{"model": SessionModel}
		body["status"] = "idle"
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer server.Close()
	c := clientForServer(server, nil)
	remote, err := c.CreateTemplate(context.Background(), fields)
	if err != nil {
		t.Fatal(err)
	}
	if !TemplateMatches(fields, remote) {
		t.Fatal("redacted readback must match public configuration")
	}
	safeJSON, _ := json.Marshal(remote)
	if bytes.Contains(safeJSON, []byte("protected-placeholder")) || bytes.Contains(safeJSON, []byte("mkdir -p")) {
		t.Fatal("protected readback escaped projection")
	}
	delete(fields, "name")
	fields[typeField] = openaiHostedType
	fields["container_size"] = testSmallContainerSize
	fields["environment_template_id"] = testHostedTemplateID
	spend := &agentsv1alpha1.SessionSpendControl{Mode: sessionSpendModeUnlimited}
	session, err := c.CreateConfiguredSession(context.Background(), "uid-test", "test-scope", "agent_test", fields, nil, nil, nil, spend)
	if err != nil {
		t.Fatal(err)
	}
	if session.EnvironmentType != openaiHostedType || session.EnvironmentConfigurationDigest != ObservableEnvironmentDigest(fields) || posts != 2 {
		t.Fatal("hosted readback mismatch")
	}
}

func TestTemplateLostCreateAndBoundedAudit(t *testing.T) {
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			posts++
			_, _ = io.WriteString(w, `{"id":"tmpl_test","name":"audit"}`)
			return
		}
		_, _ = io.WriteString(w, `{"data":[{"id":"tmpl_test","name":"audit"}],"has_more":false}`)
	}))
	defer server.Close()
	drop := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		res, err := http.DefaultTransport.RoundTrip(r)
		if err == nil && r.Method == http.MethodPost {
			_, _ = io.Copy(io.Discard, res.Body)
			_ = res.Body.Close()
			return nil, errors.New("private detail")
		}
		return res, err
	})
	c := clientForServer(server, drop)
	_, err := c.CreateTemplate(context.Background(), map[string]any{"name": "audit"})
	if err == nil || strings.Contains(err.Error(), "private detail") {
		t.Fatal("unknown create must be sanitized")
	}
	ids, err := c.FindTemplateCandidates(context.Background(), "audit")
	if err != nil || !reflect.DeepEqual(ids, []string{testHostedTemplateID}) || posts != 1 {
		t.Fatal("single POST and bounded audit contract failed")
	}
}

func TestTemplateDiscoveryRequiresCompleteScan(t *testing.T) {
	tests := []struct {
		name, body string
		wantError  bool
		wantIDs    int
	}{
		{name: "missing pagination", body: `{"data":[{"id":"one","name":"audit"}]}`, wantError: true},
		{name: "incomplete empty page", body: `{"data":[],"has_more":true}`, wantError: true},
		{name: "duplicates remain audit evidence", body: `{"data":[{"id":"one","name":"audit"},{"id":"two","name":"audit"}],"has_more":false}`, wantIDs: 2},
		{name: "missing", body: `{"data":[],"has_more":false}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tt.body)
			}))
			defer server.Close()
			ids, err := clientForServer(server, nil).FindTemplateCandidates(context.Background(), "audit")
			if (err != nil) != tt.wantError || len(ids) != tt.wantIDs {
				t.Fatalf("ids=%d err=%v", len(ids), err)
			}
		})
	}
}

func archiveFixture(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for name, content := range files {
		file, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(file, content); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
func TestPinnedHostedCapabilitiesAndArchiveSafety(t *testing.T) {
	plugin := archiveFixture(t, map[string]string{"helper/.codex-plugin/plugin.json": `{"name":"helper","description":"Test helper"}`, "helper/.mcp.json": `{"mcpServers":{}}`})
	skill := archiveFixture(t, map[string]string{"helper/SKILL.md": "---\nname: helper\ndescription: Test helper\n---\nRead files.\n"})
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{UID: "source-uid", ResourceVersion: "7"}, Immutable: new(true), Type: corev1.SecretTypeOpaque, Data: map[string][]byte{testPluginKey: plugin, testSkillKey: skill}}
	capability := agentsv1alpha1.HostedInlineCapability{Name: "helper", Description: "Test helper", Source: agentsv1alpha1.HostedArchive{SecretKey: testPluginKey, SecretUID: "source-uid", SecretResourceVersion: "7"}}
	wire, err := hostedArchive(capability, secret, true)
	if err != nil {
		t.Fatal(err)
	}
	source, ok := wire["source"].(map[string]any)
	if !ok || source["data"] != base64.StdEncoding.EncodeToString(plugin) {
		t.Fatal("archive bytes changed")
	}
	capability.Source.SecretKey = testSkillKey
	if _, err := hostedArchive(capability, secret, false); err != nil {
		t.Fatal(err)
	}
	secret.ResourceVersion = "8"
	if _, err := hostedArchive(capability, secret, false); err == nil {
		t.Fatal("unpinned source accepted")
	}
	bad := archiveFixture(t, map[string]string{"helper/../escape": "bad"})
	if validateArchive(bad, "helper", "Test helper", true) == nil {
		t.Fatal("archive traversal accepted")
	}
}

func TestArchiveExpansionLimit(t *testing.T) {
	const manifest = "---\nname: helper\ndescription: Test helper\n---\n"
	const expansionLimit = 8 * 1024 * 1024
	tests := []struct {
		name        string
		payloadSize uint64
		rawMetadata bool
		wantError   bool
	}{
		{name: "exact limit", payloadSize: expansionLimit - uint64(len(manifest))},
		{name: "over limit", payloadSize: expansionLimit - uint64(len(manifest)) + 1, wantError: true},
		{
			name:        "zip64 size wraps total",
			payloadSize: math.MaxUint64 - uint64(len(manifest)) + 1,
			rawMetadata: true,
			wantError:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			writer := zip.NewWriter(&buffer)
			file, err := writer.Create("helper/SKILL.md")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(file, manifest); err != nil {
				t.Fatal(err)
			}
			if tt.rawMetadata {
				// Malformed ZIP64 metadata must be rejected before its size wraps.
				_, err = writer.CreateRaw(&zip.FileHeader{
					Name:               "helper/payload",
					Method:             zip.Store,
					UncompressedSize64: tt.payloadSize,
				})
			} else {
				file, err = writer.Create("helper/payload")
				if err == nil {
					_, err = file.Write(make([]byte, int(tt.payloadSize)))
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			err = validateArchive(buffer.Bytes(), "helper", "Test helper", false)
			if (err != nil) != tt.wantError {
				t.Fatalf("validateArchive() error = %v, want error %t", err, tt.wantError)
			}
		})
	}
}

func TestHostedRestrictions(t *testing.T) {
	tests := []struct {
		name   string
		change func(*agentsv1alpha1.HostedEnvironment)
	}{
		{name: "reserved env", change: func(h *agentsv1alpha1.HostedEnvironment) {
			h.Env = []agentsv1alpha1.HostedVariable{{Name: "OPENAI_API_KEY", SecretKey: "x"}}
		}},
		{name: "file traversal", change: func(h *agentsv1alpha1.HostedEnvironment) {
			h.Files = []agentsv1alpha1.HostedFile{{Type: agentsv1alpha1.HostedFileTypeFileID, FileID: testFileKey, Path: "/workspace/../etc/key"}}
		}},
		{name: "latest skill", change: func(h *agentsv1alpha1.HostedEnvironment) {
			h.Skills = []agentsv1alpha1.HostedSkill{{Type: agentsv1alpha1.HostedSkillTypeSkillReference, SkillID: testSkillKey, Version: "latest"}}
		}},
		{name: "conflicting network", change: func(h *agentsv1alpha1.HostedEnvironment) {
			h.Network = agentsv1alpha1.HostedNetwork{Access: restrictedNetworkAccess, AllowedDomains: []string{"a.com"}, BlockedDomains: []string{"b.com"}}
		}},
		{name: "wildcard network", change: func(h *agentsv1alpha1.HostedEnvironment) {
			h.Network = agentsv1alpha1.HostedNetwork{Access: restrictedNetworkAccess, AllowedDomains: []string{"*.com"}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &agentsv1alpha1.HostedEnvironment{Network: agentsv1alpha1.HostedNetwork{Access: "disabled"}}
			tt.change(h)
			if _, err := HostedConfiguration(h, nil, ""); err == nil {
				t.Fatal("restriction not enforced")
			}
		})
	}
}

func TestEnvironmentProviderCancellationAndRedaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	serverCancelled := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer serverCancelled.Close()
	c := clientForServer(serverCancelled, nil)
	if _, err := c.GetTemplate(ctx, "id"); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation identity lost")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		_, _ = io.WriteString(w, "private-body")
	}))
	defer server.Close()
	if _, err := clientForServer(server, nil).GetTemplate(context.Background(), "id"); err == nil || strings.Contains(err.Error(), "private-body") {
		t.Fatal("provider error was not redacted")
	}
}

func TestHostedPluginNetworkRequirement(t *testing.T) {
	plugin := archiveFixture(t, map[string]string{"helper/.mcp.json": `{"mcpServers":{"local":{"command":"node"}}}`})
	if validatePluginNetwork(plugin, "disabled") == nil || validatePluginNetwork(plugin, restrictedNetworkAccess) == nil {
		t.Fatal("hosted stdio network requirement was dropped")
	}
	if err := validatePluginNetwork(plugin, enabledField); err != nil {
		t.Fatal(err)
	}
}

func TestMCPEnvironmentPlacement(t *testing.T) {
	local := &agentsv1alpha1.EnvironmentSpec{Mode: "SelfHosted", SelfHosted: &agentsv1alpha1.SelfHostedEnvironment{CapabilityDirectories: []string{testPinnedCapabilityPath}}}
	disabledHosted := &agentsv1alpha1.EnvironmentSpec{Mode: "OpenAIHosted", OpenAIHosted: &agentsv1alpha1.HostedEnvironment{Network: agentsv1alpha1.HostedNetwork{Access: "disabled"}}}
	tests := []struct {
		name       string
		profile    *agentsv1alpha1.EnvironmentSpec
		transport  map[string]any
		origin     string
		wantReason string
	}{
		{name: "pinned local executable", profile: local, transport: map[string]any{typeField: stdioTransportType, testCommandField: "/capabilities/helper/server", testCwdField: testPinnedCapabilityPath}},
		{name: "unowned executable", profile: local, transport: map[string]any{typeField: stdioTransportType, testCommandField: "/workspace/server", testCwdField: testPinnedCapabilityPath}, wantReason: environmentCapabilitiesReason},
		{name: "cwd traversal", profile: local, transport: map[string]any{typeField: stdioTransportType, testCommandField: "/capabilities/helper/server", testCwdField: "/capabilities/helper/../other"}, wantReason: environmentCapabilitiesReason},
		{name: "service HTTP ignores runtime network", profile: disabledHosted, transport: map[string]any{typeField: httpTransportType}, origin: testServiceOrigin},
		{name: "environment HTTP needs network", profile: disabledHosted, transport: map[string]any{typeField: httpTransportType}, origin: testEnvironmentField, wantReason: "EnvironmentNetworkRequired"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := map[string]any{toolsField: []any{map[string]any{typeField: mcpToolType, transportField: tt.transport, testConnectionOriginField: tt.origin}}}
			reason, _ := AgentEnvironmentBlock(fields, tt.profile)
			if reason != tt.wantReason {
				t.Fatalf("got reason %s", reason)
			}
		})
	}
}
