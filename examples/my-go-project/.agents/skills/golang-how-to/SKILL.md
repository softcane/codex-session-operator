---
name: golang-how-to
description: Apply this project's Go conventions and verification commands when changing Go code.
---

Read AGENTS.md and docs/architecture.md. Keep HTTP wiring in cmd/service and business
behavior in internal/job. Use the standard library. Keep map access under the store
mutex and preserve request identity semantics. Add behavior-focused regression tests,
format changed Go files with gofmt, then run make verify, including its race check.
Use the investigate, fix-bug and review-change skills for the requested task mode.
