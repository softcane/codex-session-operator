# Job submissions

The HTTP entry point delegates to the `internal/job` package. The job store serializes
map access with a mutex. Each accepted request has a caller-provided request ID and
a server-generated job ID. Repeating the same request ID must return the original
job without allocating another job. Distinct request IDs create distinct jobs.

This teaching project keeps jobs in memory. The agent's Git checkout persists on
the Session workspace PVC; that does not give the application a persistent database.
