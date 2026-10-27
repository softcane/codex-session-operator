CONTROLLER_IMG ?= controller:latest
GATEWAY_IMG ?= gateway:latest
EXECUTOR_IMG ?=
SANDBOX_POOLS ?=
EXECUTOR_BUILD_IMG ?= codex-executor:0.162.0-alpha.20
APPLICATION_EXECUTOR_IMG ?= my-go-project-executor:latest
CONTAINER_TOOL ?= docker
PLATFORMS ?= linux/amd64,linux/arm64
export GOTOOLCHAIN ?= $(shell awk '$$1 == "toolchain" {print $$2; exit}' go.mod)
YEAR ?= $(shell date +%Y)
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General
.PHONY: help
help: ## Display supported commands.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <target>\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  %-24s %s\n", $$1, $$2 } /^##@/ { printf "\n%s\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Development
.PHONY: manifests generate fmt vet
manifests: controller-gen ## Generate CRDs and RBAC from API markers.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases
generate: controller-gen ## Generate DeepCopy methods.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt",year=$(YEAR) paths="./..."
fmt: ## Format Go source.
	go fmt ./...
vet: ## Run go vet.
	go vet ./...

GO_TEST_FLAGS ?= -race -shuffle=on -count=1
.PHONY: test test-helpers test-storage-postgres test-e2e test-sandbox-live demo
test: manifests generate fmt vet setup-envtest ## Run Go unit and Kubernetes envtest tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $(GO_TEST_FLAGS) ./... -coverprofile cover.out
test-helpers: ## Test deployment helpers and the simulated provider; requires Node.js.
	python3 -B -m unittest discover -s test/e2e -p 'test_*.py'
test-storage-postgres: ## Run Go database unit/fault/crash tests against disposable PostgreSQL.
	python3 -B test/postgres.py
test-e2e: manifests generate kustomize test-helpers ## Run one isolated Kind suite: conversations and all six CRD lifecycles; no API key.
	python3 -B test/e2e/run.py $(E2E_ARGS)
test-sandbox-live: manifests generate kustomize ## Verify a real repository using restricted OpenAI credentials and a limited budget.
	python3 -B test/e2e/live.py $(LIVE_ARGS)
demo: manifests generate kustomize test-helpers ## Run the same no-key suite and keep its successful local playground.
	python3 -B test/e2e/run.py --keep

.PHONY: lint lint-fix lint-config
lint: golangci-lint ## Run the pinned custom linter.
	"$(GOLANGCI_LINT)" run
lint-fix: golangci-lint ## Apply automatic lint fixes.
	"$(GOLANGCI_LINT)" run --fix
lint-config: golangci-lint ## Verify lint configuration.
	"$(GOLANGCI_LINT)" config verify

GOVULNCHECK_VERSION ?= v1.9.0
.PHONY: vulncheck dependencies-check
vulncheck: ## Check reachable known vulnerabilities.
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...
dependencies-check: ## Verify checksums and reject module drift.
	go mod verify
	go mod tidy -diff

##@ Build
.PHONY: build run docker-build-images docker-build-controller docker-build-gateway docker-push-controller docker-push-gateway docker-build-executor docker-build-application-executor docker-buildx-controller docker-buildx-gateway
build: manifests generate fmt vet ## Build controller, gateway, administration, client and repository helper binaries.
	go build -o bin/manager cmd/main.go
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/gateway-admin ./cmd/gateway-admin
	go build -o bin/gateway-client ./cmd/gateway-client
	go build -o bin/workspace-bootstrap ./cmd/workspace-bootstrap
run: manifests generate fmt vet ## Run the controller locally against the selected kubeconfig.
	go run ./cmd/main.go
docker-build-images: docker-build-controller docker-build-gateway docker-build-executor ## Build all shared production images.
docker-build-controller: ## Build the controller-only image.
	$(CONTAINER_TOOL) build --target controller $(if $(BASE_IMAGE),--build-arg BASE_IMAGE=$(BASE_IMAGE)) -t $(CONTROLLER_IMG) .
docker-build-gateway: ## Build the gateway service and operational tools image.
	$(CONTAINER_TOOL) build --target gateway $(if $(BASE_IMAGE),--build-arg BASE_IMAGE=$(BASE_IMAGE)) -t $(GATEWAY_IMG) .
docker-push-controller: ## Push the controller image.
	$(CONTAINER_TOOL) push $(CONTROLLER_IMG)
docker-push-gateway: ## Push the gateway image.
	$(CONTAINER_TOOL) push $(GATEWAY_IMG)
docker-build-executor: ## Build the separate executor target from the same Dockerfile.
	$(CONTAINER_TOOL) build --target executor -t $(or $(EXECUTOR_IMG),$(EXECUTOR_BUILD_IMG)) .
docker-build-application-executor: docker-build-executor ## Build the my-go-project executor layer with its Go and compiler tools.
	$(CONTAINER_TOOL) build -f examples/my-go-project/sandbox/Dockerfile.executor --build-arg EXECUTOR_BASE=$(or $(EXECUTOR_IMG),$(EXECUTOR_BUILD_IMG)) -t $(APPLICATION_EXECUTOR_IMG) examples/my-go-project
docker-buildx-controller: ## Build and push the controller image for amd64 and arm64.
	$(CONTAINER_TOOL) buildx build --target controller --push --platform=$(PLATFORMS) $(if $(BASE_IMAGE),--build-arg BASE_IMAGE=$(BASE_IMAGE)) --tag $(CONTROLLER_IMG) .
docker-buildx-gateway: ## Build and push the gateway image for amd64 and arm64.
	$(CONTAINER_TOOL) buildx build --target gateway --push --platform=$(PLATFORMS) $(if $(BASE_IMAGE),--build-arg BASE_IMAGE=$(BASE_IMAGE)) --tag $(GATEWAY_IMG) .

.PHONY: build-cli-release
build-cli-release: ## Build gateway-client archives for Linux and macOS, amd64 and arm64.
	bash hack/build-cli-release.sh

.PHONY: build-installer
build-installer: manifests generate kustomize ## Render the combined controller and gateway release manifest.
	python3 -B hack/render-installer.py --kustomize "$(KUSTOMIZE)" --controller-image "$(CONTROLLER_IMG)" --gateway-image "$(GATEWAY_IMG)" $(if $(SANDBOX_POOLS),--sandbox-pools "$(SANDBOX_POOLS)") --output dist/install.yaml

##@ Deployment
.PHONY: local-up
local-up: manifests generate kustomize ## Start fresh codex-local infrastructure for hands-on real OpenAI use; creates no agent/session CRs.
	python3 -B hack/local-up.py --kustomize "$(KUSTOMIZE)" $(LOCAL_ARGS)

KUBE_CONTEXT ?=
ignore-not-found ?= false
.PHONY: install uninstall deploy undeploy
install: manifests kustomize ## Install CRDs into an explicitly selected KUBE_CONTEXT.
	@test -n "$(KUBE_CONTEXT)" || { echo 'Set KUBE_CONTEXT explicitly.' >&2; exit 1; }
	"$(KUSTOMIZE)" build config/crd | "$(KUBECTL)" --context "$(KUBE_CONTEXT)" apply -f -
uninstall: manifests kustomize ## Delete CRDs from an explicitly selected KUBE_CONTEXT.
	@test -n "$(KUBE_CONTEXT)" || { echo 'Set KUBE_CONTEXT explicitly.' >&2; exit 1; }
	"$(KUSTOMIZE)" build config/crd | "$(KUBECTL)" --context "$(KUBE_CONTEXT)" delete --ignore-not-found=$(ignore-not-found) -f -
deploy: build-installer ## Deploy dist/install.yaml into an explicitly selected KUBE_CONTEXT.
	@test -n "$(KUBE_CONTEXT)" || { echo 'Set KUBE_CONTEXT explicitly.' >&2; exit 1; }
	"$(KUBECTL)" --context "$(KUBE_CONTEXT)" apply -f dist/install.yaml
	"$(KUBECTL)" --context "$(KUBE_CONTEXT)" wait --for=condition=Established crd/agents.agents.softcane.io crd/environments.agents.softcane.io crd/profiles.agents.softcane.io crd/sessions.agents.softcane.io crd/vaultcredentials.agents.softcane.io crd/vaults.agents.softcane.io --timeout=60s
	"$(KUBECTL)" --context "$(KUBE_CONTEXT)" rollout status deployment/codex-session-operator-controller-manager --namespace codex-session-operator-system --timeout=5m
	"$(KUBECTL)" --context "$(KUBE_CONTEXT)" rollout status deployment/codex-session-operator-gateway --namespace codex-session-operator-system --timeout=5m
undeploy: build-installer ## Remove operator resources from an explicitly selected KUBE_CONTEXT.
	@test -n "$(KUBE_CONTEXT)" || { echo 'Set KUBE_CONTEXT explicitly.' >&2; exit 1; }
	"$(KUBECTL)" --context "$(KUBE_CONTEXT)" delete --ignore-not-found=$(ignore-not-found) -f dist/install.yaml

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.22.0

#ENVTEST_VERSION is the controller-runtime version to use for setup-envtest, derived from go.mod
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v")

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.14.0
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
	@version="$$("$(GOLANGCI_LINT)" version)"; \
	case "$$version" in *"version $(GOLANGCI_LINT_VERSION)"*) ;; \
		*) $(MAKE) --no-print-directory -B "$(GOLANGCI_LINT)"; exit ;; esac; \
	if [ .custom-gcl.yml -nt "$(GOLANGCI_LINT)" ] || \
		! printf '%s\n' "$$version" | grep -Fq "built with $$(go env GOVERSION) "; then \
		$(MAKE) --no-print-directory -B "$(GOLANGCI_LINT)"; \
	fi
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@if [ -f .custom-gcl.yml ]; then \
		echo "Building custom golangci-lint with plugins..." && \
		GOTOOLCHAIN="$$(go env GOVERSION)" $(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	fi

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOTOOLCHAIN="$$(go env GOVERSION)" GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
