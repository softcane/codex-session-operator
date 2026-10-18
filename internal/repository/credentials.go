package repository

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// AskPass returns a protected value only for Git's prompt for the selected endpoint.
// The calling process writes it directly to Git, never to diagnostics or configuration.
func AskPass(prompt, directory, repositoryURL string) (string, error) {
	if Validate(repositoryURL, strings.Repeat("0", 40)) != nil || !filepath.IsAbs(directory) {
		return "", errors.New("repository credential request is invalid")
	}
	password := strings.HasPrefix(prompt, "Password for '")
	if !password && !strings.HasPrefix(prompt, "Username for '") {
		return "", errors.New("repository credential prompt is unsupported")
	}
	parts := strings.Split(prompt, "'")
	if len(parts) != 3 || parts[2] != ": " {
		return "", errors.New("repository credential prompt is invalid")
	}
	endpoint, err := url.Parse(parts[1])
	expected, _ := url.Parse(repositoryURL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host != expected.Host || endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		(endpoint.Path != "" && endpoint.Path != expected.Path) {
		return "", errors.New("repository credential endpoint does not match the approved source")
	}
	username := "x-access-token"
	if data, err := os.ReadFile(filepath.Join(directory, "username")); err == nil {
		username = string(data)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", errors.New("repository credential username is unavailable")
	}
	if !validCredential(username) || strings.ContainsAny(username, "@:'") {
		return "", errors.New("repository credential username is invalid")
	}
	if endpoint.User != nil {
		if _, hasPassword := endpoint.User.Password(); hasPassword || endpoint.User.Username() != username {
			return "", errors.New("repository credential prompt identity is invalid")
		}
	}
	if !password {
		return username, nil
	}
	data, err := os.ReadFile(filepath.Join(directory, "token"))
	if err != nil || !validCredential(string(data)) {
		return "", errors.New("repository credential token is unavailable or invalid")
	}
	return string(data), nil
}

func validCredential(value string) bool {
	return len(value) > 0 && len(value) <= 65536 && utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}
