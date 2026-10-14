IMG ?= controller:latest
EXECUTOR_IMG ?=
EXECUTOR_BUILD_IMG ?= codex-executor:0.162.0-alpha.20
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
.PHONY: test test-helpers test-storage-postgres test-e2e demo
test: manifests generate fmt vet setup-envtest ## Run Go unit and Kubernetes envtest tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $(GO_TEST_FLAGS) ./... -coverprofile cover.out
test-helpers: ## Test the public no-key clients and provider fixture; requires Node.js.
	python3 -B -m unittest discover -s test/e2e -p 'test_*.py'
test-storage-postgres: ## Run Go database unit/fault/crash tests against disposable PostgreSQL.
	python3 -B test/postgres.py
test-e2e: manifests generate kustomize test-helpers ## Run one isolated Kind suite: conversations and all five CRD lifecycles; no API key.
	python3 -B test/e2e/run.py $(E2E_ARGS)
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
.PHONY: build run docker-build docker-push docker-build-executor docker-buildx
build: manifests generate fmt vet ## Build manager, gateway and store administration binaries.
	go build -o bin/manager cmd/main.go
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/gateway-store ./cmd/gateway-store
run: manifests generate fmt vet ## Run the controller locally against the selected kubeconfig.
	go run ./cmd/main.go
docker-build: ## Build the operator image containing all three binaries.
	$(CONTAINER_TOOL) build --target operator $(if $(BASE_IMAGE),--build-arg BASE_IMAGE=$(BASE_IMAGE)) -t $(IMG) .
docker-push: ## Push the operator image.
	$(CONTAINER_TOOL) push $(IMG)
docker-build-executor: ## Build the separate executor target from the same Dockerfile.
	$(CONTAINER_TOOL) build --target executor -t $(or $(EXECUTOR_IMG),$(EXECUTOR_BUILD_IMG)) .
docker-buildx: ## Build and push the operator image for amd64 and arm64.
	$(CONTAINER_TOOL) buildx build --target operator --push --platform=$(PLATFORMS) $(if $(BASE_IMAGE),--build-arg BASE_IMAGE=$(BASE_IMAGE)) --tag $(IMG) .

.PHONY: build-installer build-gateway-installer
build-installer: manifests generate kustomize ## Render dist/install.yaml without modifying source overlays; optional executor digest.
	python3 -B hack/render-installer.py --kustomize "$(KUSTOMIZE)" --image "$(IMG)" $(if $(EXECUTOR_IMG),--executor-image "$(EXECUTOR_IMG)") --overlay default --output dist/install.yaml
build-gateway-installer: manifests generate kustomize ## Render dist/gateway.yaml with operator, gateway and sample work routes; provision PostgreSQL separately.
	python3 -B hack/render-installer.py --kustomize "$(KUSTOMIZE)" --image "$(IMG)" $(if $(EXECUTOR_IMG),--executor-image "$(EXECUTOR_IMG)") --overlay work --output dist/gateway.yaml

##@ Deployment
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
