package openaiapi

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"reflect"
	"strings"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

const capabilityDirectoriesField = "capability_directories"

const (
	invalidConfigurationReason = "InvalidConfiguration"
	pathField                  = "path"
	versionField               = "version"
	networkField               = "network"
	skillsField                = "skills"
)

const (
	allowedDomainsField           = "allowed_domains"
	createOperation               = "create"
	invalidAgentToolsMessage      = "Agent returned invalid tool configuration"
	environmentCapabilitiesReason = "EnvironmentCapabilitiesRequired"
	inlineType                    = "inline"
	fileIDField                   = "file_id"
	skillIDField                  = "skill_id"
	filesField                    = "files"
	pluginsField                  = "plugins"
	restrictedNetworkAccess       = "restricted"
	openaiHostedType              = "openai_hosted"
)

// EnvironmentSecretName bounds provider secret reads to platform provisioning.
const EnvironmentSecretName = "codex-environment"

// Template carries safe readback only. Config must never contain protected values.
type Template struct {
	ID, Name string
	Config   map[string]any
}

// HostedRuntime distinguishes provisioning acceptance from completed setup.
type HostedRuntime struct {
	ID, Status string
}

// TemplateIdentity is an audit marker, not atomic remote uniqueness.
func TemplateIdentity(uid, scope string) string {
	digest := sha256.Sum256([]byte(scope))
	return " [codex:" + hex.EncodeToString(digest[:8]) + ":" + uid + "]"
}

// EnvironmentDigest includes public intent and source versions, never secret values.
func EnvironmentDigest(spec agentsv1alpha1.EnvironmentSpec, source *agentsv1alpha1.EnvironmentSourceVersion) string {
	data, _ := json.Marshal([]any{spec, source})
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// HostedNeedsSecret decides whether a fixed-name protected source is needed.
func HostedNeedsSecret(h *agentsv1alpha1.HostedEnvironment) bool {
	if h == nil {
		return false
	}
	if len(h.Env)+len(h.SetupCommands)+len(h.Plugins) > 0 {
		return true
	}
	for _, file := range h.Files {
		if file.Type == inlineType {
			return true
		}
	}
	for _, skill := range h.Skills {
		if skill.Type == inlineType {
			return true
		}
	}
	return false
}

// HostedConfiguration builds a complete replacement. It lives only in request memory.
func HostedConfiguration(h *agentsv1alpha1.HostedEnvironment, secret *corev1.Secret, name string) (map[string]any, error) {
	if h == nil {
		return nil, errors.New("hosted profile is missing")
	}
	if err := validateHosted(h); err != nil {
		return nil, err
	}
	value := func(key string) ([]byte, error) {
		if secret == nil || secret.Type != corev1.SecretTypeOpaque || len(secret.Data[key]) == 0 {
			return nil, errors.New("protected Environment source or required key is unavailable")
		}
		return secret.Data[key], nil
	}
	env := make(map[string]string, len(h.Env))
	for _, variable := range h.Env {
		data, err := value(variable.SecretKey)
		if err != nil {
			return nil, err
		}
		env[variable.Name] = string(data)
	}
	files := make([]any, 0, len(h.Files))
	for _, file := range h.Files {
		item := map[string]any{typeField: fileIDField, pathField: file.Path, fileIDField: file.FileID}
		if file.Type == inlineType {
			data, err := value(file.SecretKey)
			if err != nil {
				return nil, err
			}
			item = map[string]any{typeField: inlineType, pathField: file.Path, dataField: base64.StdEncoding.EncodeToString(data)}
		}
		files = append(files, item)
	}
	commands := make([]any, 0, len(h.SetupCommands))
	for _, command := range h.SetupCommands {
		data, err := value(command.SecretKey)
		if err != nil {
			return nil, err
		}
		commands = append(commands, map[string]any{"command": string(data), "cwd": defaultString(command.Cwd, "/workspace")})
	}
	plugins := make([]any, 0, len(h.Plugins))
	for _, plugin := range h.Plugins {
		wire, err := hostedArchive(plugin, secret, true)
		if err != nil {
			return nil, err
		}
		if err := validatePluginNetwork(secret.Data[plugin.Source.SecretKey], h.Network.Access); err != nil {
			return nil, err
		}
		plugins = append(plugins, wire)
	}
	skills := make([]any, 0, len(h.Skills))
	for _, skill := range h.Skills {
		if skill.Type == "skillReference" {
			skills = append(skills, map[string]any{typeField: "skill_reference", skillIDField: skill.SkillID, versionField: skill.Version})
			continue
		}
		wire, err := hostedArchive(*skill.Inline, secret, false)
		if err != nil {
			return nil, err
		}
		skills = append(skills, wire)
	}
	packages := &agentsv1alpha1.HostedPackages{}
	if h.Packages != nil {
		packages = h.Packages
	}
	return map[string]any{
		nameField: name, capabilityDirectoriesField: nonNilStrings(h.CapabilityDirectories),
		"desktop": map[string]any{enabledField: h.Desktop}, "env": env, filesField: files,
		networkField: map[string]any{"access": h.Network.Access, allowedDomainsField: nonNilStrings(h.Network.AllowedDomains), "blocked_domains": nonNilStrings(h.Network.BlockedDomains)},
		"packages":   map[string]any{"python": nonNilStrings(packages.Python), "system": nonNilStrings(packages.System), "npm": nonNilStrings(packages.NPM)},
		pluginsField: plugins, skillsField: skills, "setup_commands": commands,
	}, nil
}

func nonNilStrings(items []string) []string { return append([]string{}, items...) }
func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func validateHosted(h *agentsv1alpha1.HostedEnvironment) error {
	if size := h.ContainerSize; size != "" && size != "small" && size != "medium" && size != "large" {
		return errors.New("unsupported hosted container size")
	}
	if err := validateHostedNetwork(h.Network); err != nil {
		return err
	}
	for _, directory := range h.CapabilityDirectories {
		if !workspacePath(directory, false) {
			return errors.New("hosted capability path must stay inside workspace")
		}
	}
	for _, variable := range h.Env {
		if variable.Name == "PATH" || variable.Name == "OPENAI_API_KEY" || strings.HasPrefix(variable.Name, "CODEX_") {
			return errors.New("reserved hosted environment variable is restricted")
		}
	}
	for _, file := range h.Files {
		if !workspacePath(file.Path, false) || (file.Type != "fileID" && file.Type != inlineType) ||
			(file.Type == "fileID" && (file.FileID == "" || file.SecretKey != "")) || (file.Type == inlineType && (file.SecretKey == "" || file.FileID != "")) {
			return errors.New("hosted file path or branch is invalid")
		}
	}
	for _, command := range h.SetupCommands {
		if !workspacePath(defaultString(command.Cwd, "/workspace"), true) {
			return errors.New("hosted setup cwd must stay inside workspace")
		}
	}
	return validateHostedSkills(h.Skills)
}

func validateHostedNetwork(network agentsv1alpha1.HostedNetwork) error {
	n := network
	if n.Access != enabledField && n.Access != "disabled" && n.Access != restrictedNetworkAccess {
		return errors.New("hosted network policy must be explicit")
	}
	if (len(n.AllowedDomains) > 0 && len(n.BlockedDomains) > 0) || (n.Access != restrictedNetworkAccess && len(n.AllowedDomains)+len(n.BlockedDomains) > 0) ||
		(n.Access == restrictedNetworkAccess && len(n.AllowedDomains)+len(n.BlockedDomains) == 0) {
		return errors.New("hosted network domain lists conflict")
	}
	for _, domain := range append(nonNilStrings(n.AllowedDomains), n.BlockedDomains...) {
		if strings.ContainsAny(domain, "/*:@?# \\") || strings.HasPrefix(domain, ".") || strings.HasSuffix(domain, ".") || strings.Contains(domain, "..") {
			return errors.New("network domains must be exact host names")
		}
	}
	return nil
}

func validateHostedSkills(skills []agentsv1alpha1.HostedSkill) error {
	for _, skill := range skills {
		if skill.Type == "skillReference" {
			if skill.SkillID == "" || skill.Version == "" || strings.Trim(skill.Version, "0123456789") != "" || skill.Version[0] == '0' || skill.Inline != nil {
				return errors.New("skill reference requires a positive pinned version")
			}
		} else if skill.Type != inlineType || skill.Inline == nil || skill.SkillID != "" || skill.Version != "" {
			return errors.New("hosted skill branch is invalid")
		}
	}
	return nil
}

func workspacePath(value string, root bool) bool {
	return path.Clean(value) == value && (strings.HasPrefix(value, "/workspace/") || (root && value == "/workspace"))
}

func hostedArchive(capability agentsv1alpha1.HostedInlineCapability, secret *corev1.Secret, plugin bool) (map[string]any, error) {
	source := capability.Source
	if secret == nil || secret.Immutable == nil || !*secret.Immutable || string(secret.UID) != source.SecretUID || secret.ResourceVersion != source.SecretResourceVersion || len(secret.Data[source.SecretKey]) == 0 {
		return nil, errors.New("capability archive requires its pinned immutable protected source")
	}
	data := secret.Data[source.SecretKey]
	if err := validateArchive(data, capability.Name, capability.Description, plugin); err != nil {
		return nil, err
	}
	return map[string]any{typeField: inlineType, nameField: capability.Name, "description": capability.Description,
		"source": map[string]any{typeField: "base64", "media_type": "application/zip", dataField: base64.StdEncoding.EncodeToString(data)}}, nil
}

func validateArchive(data []byte, name, description string, plugin bool) error {
	const maxExpansionBytes = 8 * 1024 * 1024
	if len(data) > 2*1024*1024 {
		return errors.New("capability archive exceeds platform limit")
	}
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return errors.New("capability archive is not a ZIP")
	}
	if len(archive.File) > 256 {
		return errors.New("capability archive has too many entries")
	}
	var total uint64
	found := false
	seen := make(map[string]bool, len(archive.File))
	for _, file := range archive.File {
		clean := strings.TrimSuffix(file.Name, "/")
		if path.Clean(clean) != clean || (!strings.HasPrefix(clean, name+"/") && (clean != name || !file.FileInfo().IsDir())) || strings.Contains(clean, "\\") || file.Mode()&fs.ModeSymlink != 0 || seen[clean] {
			return errors.New("capability archive contains unsafe or duplicate paths")
		}
		seen[clean] = true
		if file.UncompressedSize64 > maxExpansionBytes-total {
			return errors.New("capability archive expansion exceeds platform limit")
		}
		total += file.UncompressedSize64
		manifest := name + "/SKILL.md"
		if plugin {
			manifest = name + "/.codex-plugin/plugin.json"
		}
		if file.Name != manifest {
			continue
		}
		reader, err := file.Open()
		if err != nil {
			return errors.New("capability manifest cannot be read")
		}
		content, readErr := io.ReadAll(io.LimitReader(reader, 8193))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || len(content) > 8192 {
			return errors.New("capability manifest exceeds platform limit or cannot be read")
		}
		if plugin {
			var metadata struct {
				Name        string `json:"name"`
				Description string `json:"description"`
			}
			if json.Unmarshal(content, &metadata) != nil || metadata.Name != name || metadata.Description != description {
				return errors.New("plugin manifest does not match safe metadata")
			}
		} else if !strings.Contains(string(content), "name: "+name+"\n") || !strings.Contains(string(content), "description: "+description+"\n") {
			return errors.New("skill manifest does not match safe metadata")
		}
		found = true
	}
	if !found {
		return errors.New("capability archive is missing its root manifest")
	}
	return nil
}

// ObservableEnvironment projects only fields that the API can safely return.
func ObservableEnvironment(fields map[string]any, runtime bool) map[string]any {
	result := make(map[string]any)
	for _, key := range []string{capabilityDirectoriesField, "desktop", networkField, "packages", pluginsField, skillsField, filesField} {
		value := fields[key]
		switch key {
		case pluginsField, skillsField, filesField:
			items := make([]any, 0)
			if list, ok := value.([]any); ok {
				for _, item := range list {
					if object, ok := item.(map[string]any); ok {
						kept := make(map[string]any)
						keys := []string{typeField, nameField, "description", skillIDField, versionField}
						if key == skillsField && object[typeField] == "skill_reference" {
							keys = []string{typeField, skillIDField, versionField}
						}
						if key == filesField {
							keys = []string{typeField, pathField, fileIDField}
						}
						for _, field := range keys {
							if v, exists := object[field]; exists {
								kept[field] = v
							}
						}
						items = append(items, kept)
					}
				}
			}
			value = items
		case networkField:
			if object, ok := value.(map[string]any); ok {
				value = map[string]any{"access": object["access"], allowedDomainsField: emptyList(object[allowedDomainsField]), "blocked_domains": emptyList(object["blocked_domains"])}
			}
		}
		if runtime && key == capabilityDirectoriesField {
			continue
		}
		result[key] = value
	}
	if runtime {
		result["container_size"] = defaultStringString(fields["container_size"], "medium")
	} else {
		result[nameField] = fields[nameField]
	}
	return result
}
func emptyList(value any) any {
	if value == nil {
		return []any{}
	}
	return value
}
func defaultStringString(value any, fallback string) string {
	text, _ := value.(string)
	return defaultString(text, fallback)
}

// ObservableEnvironmentDigest never hashes protected contents.
func ObservableEnvironmentDigest(fields map[string]any) string {
	data, _ := json.Marshal(ObservableEnvironment(fields, true))
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// TemplateMatches compares redacted readback to a redacted desired projection.
func TemplateMatches(desired map[string]any, remote Template) bool {
	data, _ := json.Marshal(desired)
	var normalized map[string]any
	_ = json.Unmarshal(data, &normalized)
	return reflect.DeepEqual(ObservableEnvironment(normalized, false), ObservableEnvironment(remote.Config, false))
}

func environmentOptions(fields map[string]any) []option.RequestOption {
	options := make([]option.RequestOption, 0, 2+len(fields))
	options = append(options, option.WithMaxRetries(0), option.WithHeader("OpenAI-Beta", agentsBetaHeader))
	for key, value := range fields {
		options = append(options, option.WithJSONSet(key, value))
	}
	return options
}

func (c *Client) CreateTemplate(ctx context.Context, fields map[string]any) (Template, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	remote, err := c.SDK.Beta.Agents.Environments.Templates.New(ctx, openai.BetaAgentEnvironmentTemplateNewParams{}, environmentOptions(fields)...)
	if err != nil {
		return Template{}, safeEnvironmentError(createOperation, err)
	}
	return summarizeTemplate(remote)
}
func (c *Client) GetTemplate(ctx context.Context, id string) (Template, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	remote, err := c.SDK.Beta.Agents.Environments.Templates.Get(ctx, id, option.WithMaxRetries(0))
	if err != nil {
		return Template{}, safeEnvironmentError("retrieve", err)
	}
	return summarizeTemplate(remote)
}
func (c *Client) UpdateTemplate(ctx context.Context, id string, fields map[string]any) (Template, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	remote, err := c.SDK.Beta.Agents.Environments.Templates.Update(ctx, id, openai.BetaAgentEnvironmentTemplateUpdateParams{}, environmentOptions(fields)...)
	if err != nil {
		return Template{}, safeEnvironmentError(updateOperation, err)
	}
	return summarizeTemplate(remote)
}

// FindTemplateCandidates returns bounded audit evidence, never ownership proof.
func (c *Client) FindTemplateCandidates(ctx context.Context, name string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	page, err := c.SDK.Beta.Agents.Environments.Templates.List(ctx, openai.BetaAgentEnvironmentTemplateListParams{Limit: openai.Int(100)}, option.WithMaxRetries(0))
	var ids []string
	for n := 0; page != nil && n < maxDiscoveryPages; n++ {
		if err != nil {
			return nil, safeEnvironmentError("discover", err)
		}
		for _, remote := range page.Data {
			if remote.Name == name {
				ids = append(ids, remote.ID)
			}
		}
		if !page.JSON.HasMore.Valid() {
			return nil, errors.New("template discovery pagination is incomplete")
		}
		if !page.HasMore {
			return ids, nil
		}
		if len(page.Data) == 0 || n+1 == maxDiscoveryPages {
			return nil, errors.New("template discovery limit reached; audit required")
		}
		page, err = page.GetNextPage()
	}
	if err != nil {
		return nil, safeEnvironmentError("discover", err)
	}
	return nil, errors.New("template discovery returned no complete page")
}
func (c *Client) DeleteTemplate(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	remote, err := c.SDK.Beta.Agents.Environments.Templates.Delete(ctx, id, option.WithMaxRetries(0))
	if err != nil {
		clean := safeEnvironmentError(deleteOperation, err)
		if errors.Is(clean, ErrNotFound) {
			return nil
		}
		return clean
	}
	if remote == nil || remote.ID != id || !remote.Deleted {
		return errors.New("template deletion was not confirmed")
	}
	return nil
}
func (c *Client) GetHostedRuntime(ctx context.Context, id string) (HostedRuntime, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	remote, err := c.SDK.Beta.Agents.Environments.Get(ctx, id, option.WithMaxRetries(0))
	if err != nil {
		return HostedRuntime{}, safeEnvironmentError("retrieve", err)
	}
	return HostedRuntime{ID: remote.ID, Status: string(remote.Status)}, nil
}
func summarizeTemplate(remote *openai.EnvironmentTemplate) (Template, error) {
	if remote == nil || remote.ID == "" {
		return Template{}, errors.New("template response contains no identity")
	}
	var fields map[string]any
	if json.Unmarshal([]byte(remote.RawJSON()), &fields) != nil {
		return Template{}, errors.New("template response contains invalid configuration")
	}
	return Template{ID: remote.ID, Name: remote.Name, Config: ObservableEnvironment(fields, false)}, nil
}
func safeEnvironmentError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if apiErr, ok := errors.AsType[*openai.Error](err); ok {
		if apiErr.StatusCode == 404 && operation != createOperation {
			return ErrNotFound
		}
		return fmt.Errorf("OpenAI Environment %s failed (HTTP %d)", operation, apiErr.StatusCode)
	}
	return fmt.Errorf("OpenAI Environment %s failed (transport or response error)", operation)
}

// validatePluginNetwork catches the hosted stdio requirement before any remote write.
func validatePluginNetwork(data []byte, networkAccess string) error {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return errors.New("plugin archive is invalid")
	}
	for _, file := range archive.File {
		if !strings.HasSuffix(file.Name, "/.mcp.json") {
			continue
		}
		reader, err := file.Open()
		if err != nil {
			return errors.New("plugin MCP configuration cannot be read")
		}
		contents, readErr := io.ReadAll(io.LimitReader(reader, 65537))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || len(contents) > 65536 {
			return errors.New("plugin MCP configuration exceeds platform limit or cannot be read")
		}
		var config struct {
			Servers map[string]struct {
				Type    string `json:"type"`
				Command string `json:"command"`
			} `json:"mcpServers"`
		}
		if json.Unmarshal(contents, &config) != nil {
			return errors.New("plugin MCP configuration is invalid")
		}
		for _, server := range config.Servers {
			if (server.Type == stdioTransportType || server.Command != "") && networkAccess != enabledField {
				return errors.New("hosted stdio plugins require enabled network access")
			}
		}
	}
	return nil
}

// TemplateConfigurationDigest excludes settings owned only by Session creation.
func TemplateConfigurationDigest(hosted *agentsv1alpha1.HostedEnvironment, source *agentsv1alpha1.EnvironmentSourceVersion) string {
	copyHosted := hosted.DeepCopy()
	copyHosted.ContainerSize = ""
	return EnvironmentDigest(agentsv1alpha1.EnvironmentSpec{Mode: "openaiHosted", OpenAIHosted: copyHosted}, source)
}

// ManagedTemplateName preserves the ownership marker even when the display prefix is cleared.
func ManagedTemplateName(prefix, uid, scope string) string {
	if prefix == "" {
		prefix = "codex-environment"
	}
	return prefix + TemplateIdentity(uid, scope)
}
