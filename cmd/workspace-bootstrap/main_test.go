package main

import (
	"bytes"
	"testing"
)

func TestBootstrapCLIHelpAndSafeArgumentFailures(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		code int
	}{
		{name: "help", args: []string{"--help"}},
		{name: "unknown option", args: []string{"--private-token"}, code: 2},
		{name: "extra argument", args: []string{"extra"}, code: 2},
		{name: "invalid timeout", args: []string{"--timeout", "0"}, code: 2},
		{name: "missing configuration", code: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var output, diagnostics bytes.Buffer
			if got := run(t.Context(), tt.args, &output, &diagnostics); got != tt.code {
				t.Fatalf("exit = %d, want %d", got, tt.code)
			}
			if bytes.Contains(diagnostics.Bytes(), []byte("private-token")) {
				t.Fatal("argument text leaked into diagnostics")
			}
			if tt.code != 0 && output.Len() != 0 {
				t.Fatal("failed preparation printed success output")
			}
		})
	}
}
