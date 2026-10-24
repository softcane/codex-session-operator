# Contributing

Read [README.md](README.md), [architecture](docs/architecture.md) and the [conversation contract](docs/conversations.md) before changing behavior. For bug reports, include the software and Kubernetes versions, reproduction steps, expected behavior and redacted logs. Report vulnerabilities through [SECURITY.md](SECURITY.md).

## Development

Use the Go toolchain in [go.mod](go.mod). Run `make help` to list supported commands. Make installs pinned tools into the ignored `bin/` directory.

For Go changes, run:

```sh
make dependencies-check
make lint
make test
```

`make test` includes race detection, shuffled order and Kubernetes envtest. Add verification for the behavior you changed: `make test-helpers` for deployment helpers, `make test-storage-postgres` for transactions and recovery, and `make test-e2e` for deployed lifecycle behavior. The [development guide](docs/development.md#choose-checks-for-your-change) lists prerequisites and what each check covers.

The end-to-end suite creates its own isolated Kind cluster, uses a simulated provider and makes no model calls. `make demo` retains the successful deployment for inspection.

The separate `make test-sandbox-live` gate uses restricted OpenAI credentials and incurs charges. Run it only when real-provider verification is authorized. Read [Sandbox qualification](docs/development.md#sandbox-qualification) for spending modes, retained deployments and cleanup.

## Code and configuration

Use the [architecture source map](docs/architecture.md#source-map) to find the component that owns the behavior. Read [AGENTS.md](AGENTS.md) for repository contract rules.

Edit API source, then run `make manifests generate`. Keep examples and documentation aligned with the generated schemas. Delegate Pod/PVC creation to Agent Sandbox and preserve the gateway's delivery checks. Store conversation content in PostgreSQL, keep credential values out of reports and status, and preserve claims/finalizers when a remote outcome is uncertain.

In your pull request, explain the problem, the resulting behavior and the checks you ran. Update runnable examples and public contracts alongside the implementation. Report any verification you could not complete. Contributions use [Apache 2.0](LICENSE).

## Releases

The [release workflow](.github/workflows/release.yml) runs the shared quality gates before publishing a `vX.Y.Z` tag, with an optional prerelease suffix such as `v0.1.0-rc.1`. It builds three Dockerfile targets for Linux amd64 and arm64:

| Image | Registry name |
| --- | --- |
| Controller | `ghcr.io/<owner>/<repository>:<version-tag>` |
| Gateway | `ghcr.io/<owner>/<repository>-gateway:<version-tag>` |
| Executor | `ghcr.io/<owner>/<repository>-executor:<version-tag>` |

Registry names use the lowercase GitHub repository name. Releases contain `install.yaml` for the controller and gateway, `images.json` with all three immutable image references and the source revision, and `SHA256SUMS`. Administrators install Agent Sandbox, provision PostgreSQL and Secrets, and approve their own versioned Sandbox templates/pools. The release manifest ships no application Sandbox recipe.

Enable GitHub Actions and grant its token package and release publication permissions. Public pulls require public visibility for all three GHCR packages. Protect release tags and require CI before merging. The workflow refuses to replace an existing release; publish a new version for corrections. Branch and pull-request runs do not publish releases.
