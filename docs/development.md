# Development

Use Go matching `go.mod`, Make and Node.js for the local browser regression tests. The end-to-end suite also needs Docker, Python 3, Kind, `kubectl` and OpenSSL. Make installs pinned Kustomize, controller-gen, envtest and lint tools into `bin/` when their targets need them.

## Checks

```sh
make build
make lint
make test
make test-e2e
```

`make test` runs Go tests with race detection and shuffled order, including controller envtest. The gateway tests cover replay, storage boundaries and browser retry behavior. Database integration cases require a disposable PostgreSQL instance; run the optional gate below for real transaction, crash and restore checks:

```sh
make test-storage-postgres
```

`make test-e2e` builds one isolated Kind deployment and tests the resource and conversation lifecycle against a simulated provider. It writes a private report outside the repository and removes its cluster. It does not use an OpenAI API key. `make demo` retains the same deployment and prints its UI, client and cleanup instructions.

## Editing

Edit API source in `api/v1alpha1/`, then regenerate:

```sh
make manifests generate
```

Do not edit generated CRDs, RBAC or deepcopy code by hand. Keep Kubebuilder markers and `PROJECT` intact. Update the resource samples and public documentation when an API contract changes.

Match verification to the boundary you change: controller unit/envtest for reconciliation, provider wire tests for request mapping, PostgreSQL tests for durability, and the isolated end-to-end suite for deployed behavior. Use explicit Kubernetes contexts for manual work.

Keep prompts, transcripts, approvals, receipts and replay keys in the gateway store. CR status contains remote identities, conditions and safe snapshots. Before a remote create, persist its claim and recover ambiguous results by verified ownership. Do not bypass uncertainty by clearing claims or finalizers.

Self-hosted Pods and workspace claims belong to the Session UID. Validate approved image digests, StorageClasses and Secret references. Preserve `Delete` and `Retain` behavior, and stop fresh gateway input before cleanup.

## Images and release

The single [Dockerfile](../Dockerfile) provides `operator`, `executor` and `test-provider` targets. Build the default operator image with:

```sh
make docker-build IMG=codex-session-operator:local
```

The operator image includes the manager, gateway and offline store administration binaries. The gateway Deployment starts `/gateway`; the controller Deployment starts `/manager`.

Review the [release workflow](../.github/workflows/release.yml) before creating a version tag such as `v0.1.0` or `v0.1.0-rc.1`. It runs the quality gates for that tag, then publishes operator and executor images to the repository's GitHub Container Registry namespace for `linux/amd64` and `linux/arm64`. Image tags preserve the full version tag; the workflow does not publish `latest`.

The GitHub Release includes `install.yaml` (operator only), `gateway.yaml` (full profile gateway overlay), `images.json` (immutable image references and source revision), and `SHA256SUMS`. PostgreSQL and credential provisioning remain deployment responsibilities. Keep local plans, credentials, reports and experimental harnesses in the ignored `.private/` directory.
