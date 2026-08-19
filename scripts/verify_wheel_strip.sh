#!/usr/bin/env bash
# Verify the wheel-slimming impact of `strip = "symbols"`.
#
# Measures the real shipped artifact: strips the daft.abi3.so from an
# already-published wheel (built by CI with the release-lto profile) and reports
# the before/after delta. cargo's strip="symbols" is a deterministic post-link
# `strip` call, so this reproduces exactly what the release profile now emits --
# no local rebuild, and no release-vs-release-lto profile mismatch.
#
# Usage: scripts/verify_wheel_strip.sh [wheel.whl | daft.abi3.so]
#        (no arg -> downloads the latest daft wheel from PyPI)
set -euo pipefail
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT

in=${1:-}
if [ -z "$in" ]; then
  echo "no input -> downloading latest daft wheel from PyPI..." >&2
  python3 -m pip download daft --no-deps --only-binary=:all: -d "$tmp/dl" >&2
  in=$(ls "$tmp/dl"/*.whl | head -1)
fi

case "$in" in
  *.whl) unzip -q -o "$in" -d "$tmp" 'daft/daft.abi3.so'; so="$tmp/daft/daft.abi3.so" ;;
  *)     so="$in" ;;
esac

# Match cargo strip="symbols" per platform: macOS `strip -x`, Linux `strip --strip-unneeded`.
cp "$so" "$tmp/after.so"
case "$(uname)" in
  Darwin) strip -x "$tmp/after.so" ;;
  *)      strip --strip-unneeded "$tmp/after.so" ;;
esac

row() { printf "%-8s  binary=%4s MB  gzip=%5.1f MB  symbols=%s\n" \
  "$1" "$(du -m "$2" | cut -f1)" \
  "$(gzip -c "$2" | wc -c | awk '{print $1/1048576}')" \
  "$(nm "$2" 2>/dev/null | wc -l | tr -d ' ')"; }

echo "input: $(basename "$in")"
row before "$so"
row after  "$tmp/after.so"
