# Contributing

Start with [README.md](README.md) and the [deployment guide](docs/deployment.md). For a bug report, include the version, Kubernetes version, steps to reproduce, expected behavior, and redacted logs. Follow [SECURITY.md](SECURITY.md) for vulnerabilities.

## Development

Use the Go toolchain in `go.mod`. The local demo and end to end suite also need Docker, Kind, kubectl, Python 3, Node.js, OpenSSL and Make. `make help` lists supported commands; Go build tools are installed into the ignored `bin/` directory.

```sh
make dependencies-check
make lint
make test
make test-storage-postgres
make test-e2e
```

`make test` runs Go unit tests and Kubernetes envtest with race detection and randomized order. The PostgreSQL target checks durable receipts, concurrency and crash recovery against a disposable database. There is one public end to end runner: it creates its own Kind cluster and kubeconfig, exercises conversations and all five resource lifecycles with a simulated provider, and cleans up afterward. It needs no real API key. `make demo` keeps that same successful playground for manual use.

Match verification to the change. Keep tests that protect ownership, durability, credential boundaries and cleanup. CI runs these gates for pull requests and again from each release tag.

## Code and configuration

- `api/v1alpha1/`: custom resource types and validation markers.
- `internal/controller/`: reconciliation and resource lifecycle.
- `internal/gateway/`: authenticated API, UI and durable conversation storage.
- `internal/openaiapi/`: provider adapter and ownership checks.
- `internal/sandbox/`: executor Pod and workspace validation.
- `config/`: install overlays and public examples.
- `test/e2e/`: the no-key end to end runner, fixture and helper tests.
- `hack/`: code generation headers and installer rendering.

Change API source before generated files, then run `make manifests generate`. Keep documentation and examples aligned with the implementation. Conversation inputs, receipts and history belong in the gateway database; Kubernetes status carries safe configuration and identity only. Preserve ambiguous remote outcomes and finalizers instead of bypassing them. Keep credentials and private transcripts out of test reports and examples.

Open a focused pull request explaining the behavior change and commands you ran. Contributions are licensed under [Apache 2.0](LICENSE).

## Releases

Maintainers publish by pushing a new `vX.Y.Z` tag, optionally with a SemVer prerelease suffix such as `v0.1.0-rc.1`. The release workflow first reruns the same quality gates. It then builds operator and executor targets from the one Dockerfile for Linux amd64 and arm64 and publishes them to:

- `ghcr.io/<owner>/<repository>:<version-tag>`
- `ghcr.io/<owner>/<repository>-executor:<version-tag>`

Names come from the current GitHub repository and are converted to lowercase. Releases include `install.yaml` (operator), `gateway.yaml` (operator and gateway with example routes), `images.json` (immutable image digests and source revision), and `SHA256SUMS`. Manifests require the settings and Secrets in the [deployment guide](docs/deployment.md). The executor image is separate from the operator image.

Enable GitHub Actions and allow its token to publish packages and releases. For public pulls, ensure both GHCR packages have public visibility in their package settings. Protect release tags and require the CI checks before merging. Publish a new version for corrections; the workflow refuses to replace an existing release. Prerelease tags produce prereleases. Branch and pull request runs never publish images or releases.
