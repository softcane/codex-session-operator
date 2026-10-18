# Build the controller, gateway, executor and isolated acceptance fixtures.
# Override BASE_IMAGE only with a reviewed, patched Go image.
ARG BASE_IMAGE=golang:1.27.2@sha256:007a316059246276cb214df93950be77837545dc78007a73e0291a4963d5d3bf
ARG NODE_IMAGE=node:22.22.0-bookworm-slim@sha256:dd9d21971ec4395903fa6143c2b9267d048ae01ca6d3ea96f16cb30df6187d94

# An unqualified build stage resolves for TARGETPLATFORM, so test executors copy
# the target architecture's Go toolchain rather than the builder's.
FROM ${BASE_IMAGE} AS executor-toolchain

# Build on the host architecture and cross-compile for the requested platform.
# BuildKit supplies BUILDPLATFORM, TARGETOS and TARGETARCH for plain builds too.
FROM --platform=$BUILDPLATFORM ${BASE_IMAGE} AS operator-builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
RUN test -n "$TARGETOS" && test -n "$TARGETARCH" \
    && CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -trimpath -o /out/manager ./cmd/main.go \
    && CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -trimpath -o /out/gateway ./cmd/gateway \
    && CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -trimpath -o /out/gateway-admin ./cmd/gateway-admin \
    && CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -trimpath -o /out/gateway-client ./cmd/gateway-client \
    && CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -trimpath -o /out/workspace-bootstrap ./cmd/workspace-bootstrap

# Shared execution image: application source, credentials and project toolchains are
# supplied by application-specific layers instead of being copied here.
# Pin a CLI release that exposes the self-hosted exec-server command.
FROM ${NODE_IMAGE} AS executor
LABEL org.opencontainers.image.licenses="Apache-2.0"
RUN apt-get update && apt-get install -y --no-install-recommends bash git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g @openai/codex@0.162.0-alpha.20 \
    && npm cache clean --force \
    && codex exec-server --help \
    && mkdir -p /workspace && chown node:node /workspace
COPY --from=operator-builder /out/workspace-bootstrap /workspace-bootstrap
USER 1000:1000
ENV HOME=/home/node
WORKDIR /workspace
ENTRYPOINT ["codex", "exec-server"]

# Exercise real Pods and Go project files with a deterministic execution protocol.
# The Go toolchain is test-only; production applications add their own tools in a
# separate image such as examples/my-go-project/sandbox/Dockerfile.executor.
FROM executor AS test-executor
USER 0:0
COPY --from=executor-toolchain /usr/local/go /usr/local/go
COPY --from=repository-ca ca.crt /usr/local/share/ca-certificates/repository-fixture.crt
RUN update-ca-certificates \
    && ln -s /usr/local/go/bin/go /usr/local/bin/go \
    && ln -s /usr/local/go/bin/gofmt /usr/local/bin/gofmt \
    && rm -f /usr/local/bin/codex
COPY --chown=1000:1000 --chmod=0755 test/e2e/executor.js /usr/local/bin/codex
USER 1000:1000
ENV PATH=/usr/local/go/bin:$PATH
ENV GOCACHE=/workspace/.cache/go-build GOMODCACHE=/workspace/.cache/go-mod GOPATH=/workspace/.cache/go CGO_ENABLED=0 GOTOOLCHAIN=local
ENTRYPOINT ["codex", "exec-server"]

# Private HTTPS Git transport used only by isolated acceptance tests.
FROM ${NODE_IMAGE} AS test-repository
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --chown=1000:1000 test/e2e/repository.js /fixture/repository.js
USER 1000:1000
CMD ["node", "/fixture/repository.js"]

# Deterministic local acceptance provider; never calls OpenAI.
FROM ${NODE_IMAGE} AS test-provider
LABEL io.softcane.test="agents-api-provider-fixture"
COPY --chown=1000:1000 test/e2e/provider.js /fixture/server.js
USER 1000:1000
CMD ["node", "/fixture/server.js"]

# The controller and gateway have separate images and one installation.
FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3 AS gateway
LABEL org.opencontainers.image.licenses="Apache-2.0"
WORKDIR /
COPY --from=operator-builder /out/gateway /gateway
COPY --from=operator-builder /out/gateway-admin /gateway-admin
COPY --from=operator-builder /out/gateway-client /gateway-client
USER 65532:65532
ENTRYPOINT ["/gateway"]

# Keep the controller last so a plain `docker build .` produces this image.
FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3 AS controller
LABEL org.opencontainers.image.licenses="Apache-2.0"
WORKDIR /
COPY --from=operator-builder /out/manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
