package repository

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAskPassCredentialScopeAndRedaction(t *testing.T) {
	dir := t.TempDir()
	const token = "controlled-private-token"
	if err := os.WriteFile(filepath.Join(dir, "token"), []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, prompt, want string
		invalid            bool
	}{
		{name: "username", prompt: "Username for 'https://github.com': ", want: "x-access-token"},
		{name: "password", prompt: "Password for 'https://x-access-token@github.com': ", want: token},
		{name: "foreign host", prompt: "Password for 'https://foreign.example': ", invalid: true},
		{name: "redirect path", prompt: "Password for 'https://github.com/foreign': ", invalid: true},
		{name: "foreign identity", prompt: "Password for 'https://foreign@github.com': ", invalid: true},
		{name: "unsupported prompt", prompt: "print token", invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			value, err := AskPass(tt.prompt, dir, _testURL)
			if (err != nil) != tt.invalid || value != tt.want || err != nil && strings.Contains(err.Error(), token) {
				t.Fatal("credential scope or error redaction failed")
			}
		})
	}
	if err := os.WriteFile(filepath.Join(dir, "token"), []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := AskPass("Password for 'https://github.com': ", dir, _testURL); err == nil || strings.Contains(err.Error(), token) {
		t.Fatal("invalid protected credential accepted or disclosed")
	}
}
