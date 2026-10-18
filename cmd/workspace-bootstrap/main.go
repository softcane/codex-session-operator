package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/softcane/codex-session-operator/internal/repository"
)

const _usage = "workspace-bootstrap --storage-root PATH --session-uid UID --url HTTPS_URL " +
	"--commit SHA [--credential-directory PATH]"

func main() { os.Exit(mainCode()) }

func mainCode() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, os.Args[1:], os.Stdout, os.Stderr)
}

func run(ctx context.Context, args []string, output, diagnostics io.Writer) int {
	if len(args) == 1 && (strings.HasPrefix(args[0], "Username for '") || strings.HasPrefix(args[0], "Password for '")) {
		value, err := repository.AskPass(args[0],
			os.Getenv("WORKSPACE_BOOTSTRAP_CREDENTIAL_DIRECTORY"), os.Getenv("WORKSPACE_BOOTSTRAP_REPOSITORY_URL"))
		if err != nil {
			_, _ = fmt.Fprintln(diagnostics, err)
			return 1
		}
		if _, err := fmt.Fprintln(output, value); err != nil {
			return 1
		}
		return 0
	}
	flags := flag.NewFlagSet("workspace-bootstrap", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var config repository.Config
	flags.StringVar(&config.StorageRoot, "storage-root", "", "mounted workspace volume root")
	flags.StringVar(&config.SessionUID, "session-uid", "", "owning Session UID")
	flags.StringVar(&config.URL, "url", "", "approved credential-free HTTPS repository URL")
	flags.StringVar(&config.Commit, "commit", "", "full pinned commit")
	flags.StringVar(&config.CredentialDirectory, "credential-directory", "", "protected preparation credential mount")
	timeout := flags.Duration("timeout", 5*time.Minute, "bounded preparation timeout")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			if _, err := fmt.Fprintln(output, _usage); err != nil {
				return 1
			}
			return 0
		}
		_, _ = fmt.Fprintln(diagnostics, "invalid repository preparation arguments")
		return 2
	}
	if flags.NArg() != 0 || *timeout <= 0 || *timeout > 10*time.Minute {
		_, _ = fmt.Fprintln(diagnostics, "invalid repository preparation arguments")
		return 2
	}
	var err error
	config.AskPassExecutable, err = os.Executable()
	if err != nil {
		_, _ = fmt.Fprintln(diagnostics, "repository credential helper is unavailable")
		return 1
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if err := repository.Prepare(ctx, config); err != nil {
		_, _ = fmt.Fprintln(diagnostics, err)
		return 1
	}
	if _, err := fmt.Fprintln(output, "Repository workspace prepared"); err != nil {
		return 1
	}
	return 0
}
