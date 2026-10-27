#!/usr/bin/env bash
set -euo pipefail

release_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release_output="${1:-$release_root/dist}"
mkdir -p "$release_output"
release_output="$(cd "$release_output" && pwd)"
cli_staging="$(mktemp -d "${TMPDIR:-/tmp}/codex-cli-release.XXXXXX")"
trap 'rm -rf "$cli_staging"' EXIT
cd "$release_root"

for target_os in linux darwin; do
  for target_arch in amd64 arm64; do
    target_directory="$cli_staging/${target_os}_${target_arch}"
    mkdir -p "$target_directory"
    CGO_ENABLED=0 GOOS="$target_os" GOARCH="$target_arch" \
      go build -trimpath -ldflags='-s -w' -o "$target_directory/gateway-client" ./cmd/gateway-client
    chmod 0755 "$target_directory/gateway-client"
    cp LICENSE "$target_directory/LICENSE"
    archive="$release_output/gateway-client_${target_os}_${target_arch}.tar.gz"
    COPYFILE_DISABLE=1 LC_ALL=C tar -czf "$archive" -C "$target_directory" gateway-client LICENSE
    printf 'Built %s\n' "$archive"
  done
done
