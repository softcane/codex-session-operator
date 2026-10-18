# Working in my-go-project

Read `docs/architecture.md` before changing service behavior.
Use the registered `golang-how-to` skill before Go changes.

- `cmd/service` starts the HTTP service.
- `internal/job` owns submissions and their request identities.
- Run `make bootstrap` before working and `make verify` before reporting completion.
- A retry with the same request ID must return the original job and keep one stored job.
- Add a regression test and demonstrate its failure before repairing a bug.
- Preserve concurrency safety and existing behavior for distinct requests.
- Keep the task scope small. Do not remove or weaken tests.
- Record findings and verification evidence under `.artifacts/`; keep it out of Git.
- Commit verified source changes only when the task requests it.
- Push branches or create PRs only when the task explicitly authorizes publication.
- Do not inspect credentials, dump environment variables or print token values.

The Environment registers a read-only skill bundle under `/capabilities/my-go-project`.
Project skills in this checkout are source files for that bundle.
