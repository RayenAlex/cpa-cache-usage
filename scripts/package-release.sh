#!/usr/bin/env bash
# Build CPA plugin-store compatible release assets:
#   dist/release/<id>_<ver>_<goos>_<goarch>.zip   (contains <id>.so at the zip root)
#   dist/release/checksums.txt                   (sha256 of every zip, one per line)
# plus the plain <id>-v<ver>.so for manual installs.
# The CPA plugin store (internal/pluginstore) only recognises exactly this
# layout: ArchiveName() = "<id>_<version>_<goos>_<goarch>.zip", the library
# inside must be named "<id>.so" at the archive root, and a "checksums.txt"
# asset must list the zip's sha256.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
PLUGIN_ID="cpa-cache-usage"
VERSION="${VERSION:-$(sed -n 's/.*pluginVersion *= *"\(.*\)".*/\1/p' main.go | head -n1)}"
OUT="dist/release"; rm -rf "$OUT"; mkdir -p "$OUT"
TARGETS="${TARGETS:-linux/amd64 linux/arm64}"
for t in $TARGETS; do
  goos="${t%/*}"; goarch="${t#*/}"
  cc=""; [[ "$goarch" == "arm64" && "$(uname -m)" != "aarch64" ]] && cc="aarch64-linux-gnu-gcc"
  work="$(mktemp -d)"
  env CGO_ENABLED=1 GOOS="$goos" GOARCH="$goarch" ${cc:+CC=$cc} \
    go build -trimpath -buildvcs=false -buildmode=c-shared -ldflags="-s -w" -o "$work/${PLUGIN_ID}.so" .
  rm -f "$work/${PLUGIN_ID}.h"
  zip="${PLUGIN_ID}_${VERSION}_${goos}_${goarch}.zip"
  ( cd "$work" && zip -q -j "$OLDPWD/$OUT/$zip" "${PLUGIN_ID}.so" )
  cp "$work/${PLUGIN_ID}.so" "$OUT/${PLUGIN_ID}-v${VERSION}-${goos}-${goarch}.so"
  rm -rf "$work"
  echo "packaged $OUT/$zip"
done
( cd "$OUT" && sha256sum *.zip > checksums.txt && cat checksums.txt )
