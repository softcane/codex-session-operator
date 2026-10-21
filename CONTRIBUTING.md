# Contributing

Read [README.md](README.md), [architecture](docs/architecture.md) and the [conversation contract](docs/conversations.md) before changing behavior. For bug reports, include the software and Kubernetes versions, reproduction steps, expected behavior and redacted logs. Report vulnerabilities through [SECURITY.md](SECURITY.md).

## Development

Use the Go toolchain in `go.mod`. Docker, Kind, kubectl, Python 3.10 or newer, Node.js, OpenSSL and Make support the isolated test harness. `make help` lists commands; Make installs pinned build tools into the ignored `bin/` directory.

```sh
make dependencies-check
make lint
make test
make test-storage-postgres
make test-e2e
```

`make test` runs unit tests and Kubernetes envtest with race detection and shuffled order. Envtest installs the pinned upstream Sandbox CRD fixtures. The PostgreSQL gate checks transactions, concurrency and crash recovery against a disposable database.

The no-key end-to-end suite creates an isolated Kind cluster, installs Agent Sandbox, and exercises conversations, all six operator CRDs and upstream-managed Pods/PVCs with a simulated provider. It removes the cluster after a successful run. `make demo` keeps that deployment for inspection.

The separate `make test-sandbox-live` gate uses restricted OpenAI credentials and incurs charges. Run it only when real provider verification is authorized; it is not part of ordinary CI. Read [development](docs/development.md#sandbox-qualification) before using it.

## Code and configuration

- `api/v1alpha1/`: custom resource types and validation markers.
- `internal/controller/`: remote identity, reconciliation, Sandbox claims and cleanup.
- `internal/gateway/`: HTTP, durable conversations, history and approvals.
- `internal/openaiapi/`: provider adapter and ownership checks.
- `internal/sandbox/`: approved recipe validation and execution ownership/preparation proofs.
- `config/` and `examples/`: installation overlays, native Sandbox recipes and application examples.
- `test/e2e/`: isolated deployment tests, fixtures and helper checks.
- `hack/`: code generation headers, installer rendering and local setup.

Edit API source, then run `make manifests generate`. Keep examples and documentation aligned with the generated schemas. Delegate Pod/PVC creation to Agent Sandbox and preserve the gateway's delivery checks. Store conversation content in PostgreSQL, keep credential values out of reports and status, and preserve claims/finalizers when a remote outcome is uncertain.

Match verification to the changed boundary. Explain the resulting behavior and checks in the pull request. Contributions use [Apache 2.0](LICENSE).

## Releases

The [release workflow](.github/workflows/release.yml) runs the shared quality gates before publishing a `vX.Y.Z` tag, with an optional prerelease suffix such as `v0.1.0-rc.1`. It builds three Dockerfile targets for Linux amd64 and arm64:

| Image | Registry name |
| --- | --- |
| Controller | `ghcr.io/<owner>/<repository>:<version-tag>` |
| Gateway | `ghcr.io/<owner>/<repository>-gateway:<version-tag>` |
| Executor | `ghcr.io/<owner>/<repository>-executor:<version-tag>` |

Registry names use the lowercase GitHub repository name. Releases contain `install.yaml` for the controller and gateway, `images.json` with all three immutable image references and the source revision, and `SHA256SUMS`. Administrators install Agent Sandbox, provision PostgreSQL and Secrets, and approve their own versioned Sandbox templates/pools. The release manifest ships no application Sandbox recipe.

Enable GitHub Actions and grant its token package and release publication permissions. Public pulls require public visibility for all three GHCR packages. Protect release tags and require CI before merging. The workflow refuses to replace an existing release; publish a new version for corrections. Branch and pull-request runs do not publish releases.
