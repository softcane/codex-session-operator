// gateway-client uses the durable conversation HTTP API without Kubernetes credentials.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/softcane/codex-session-operator/internal/gatewayclient"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := gatewayclient.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	cancel()
	os.Exit(code)
}
