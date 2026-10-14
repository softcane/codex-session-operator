# One build file, three images. The operator is the default target.
# Override BASE_IMAGE only with a reviewed, patched Go image.
ARG BASE_IMAGE=golang:1.27.2@sha256:007a316059246276cb214df93950be77837545dc78007a73e0291a4963d5d3bf
ARG NODE_IMAGE=node:22.22.0-bookworm-slim@sha256:dd9d21971ec4395903fa6143c2b9267d048ae01ca6d3ea96f16cb30df6187d94

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
    && CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -trimpath -o /out/gateway-store ./cmd/gateway-store

# Separate execution image: no repository files or credentials are copied here.
# Pin a CLI release that exposes the self-hosted exec-server command.
FROM ${NODE_IMAGE} AS executor
LABEL org.opencontainers.image.licenses="Apache-2.0"
RUN apt-get update && apt-get install -y --no-install-recommends bash git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g @openai/codex@0.162.0-alpha.20 \
    && npm cache clean --force \
    && codex exec-server --help \
    && mkdir -p /workspace && chown node:node /workspace
USER 1000:1000
ENV HOME=/home/node
WORKDIR /workspace
ENTRYPOINT ["codex", "exec-server"]

# Deterministic local acceptance provider; never calls OpenAI.
FROM ${NODE_IMAGE} AS test-provider
LABEL io.softcane.test="issue-adapter-provider-fixture"
COPY --chown=1000:1000 test/e2e/provider.js /fixture/server.js
USER 1000:1000
CMD ["node", "/fixture/server.js"]

# Keep this stage last so `docker build .` produces the operator image.
FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3 AS operator
LABEL org.opencontainers.image.licenses="Apache-2.0"
WORKDIR /
COPY --from=operator-builder /out/manager /manager
COPY --from=operator-builder /out/gateway /gateway
COPY --from=operator-builder /out/gateway-store /gateway-store
USER 65532:65532
ENTRYPOINT ["/manager"]
