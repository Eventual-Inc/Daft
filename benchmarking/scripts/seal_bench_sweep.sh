#!/usr/bin/env bash
# Sweep across writer × seal × N, capture the CSV line each run emits.
# Drops page cache between runs on Linux (needs sudo); skip on macOS.
#
# Usage:
#   ./benchmarking/scripts/seal_bench_sweep.sh [BYTES] > results.csv
#
# Override per-run config via env:
#   INPUTS=200 PAYLOAD_COLS=8 MAP_CONC=16 READ_CONC=16 ./seal_bench_sweep.sh

set -euo pipefail

BIN=${BIN:-target/release/seal_bench}
BYTES=${1:-4GiB}
INPUTS=${INPUTS:-100}
PAYLOAD_COLS=${PAYLOAD_COLS:-4}
MAP_CONC=${MAP_CONC:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}
READ_CONC=${READ_CONC:-$MAP_CONC}
PREFETCH=${DAFT_SHUFFLE_READ_PREFETCH:-1}

if [[ ! -x "$BIN" ]]; then
  echo "Building $BIN..." >&2
  cargo build --release -p daft-shuffles --bin seal_bench >&2
fi

drop_caches() {
  if [[ "$(uname)" == "Linux" ]] && [[ -w /proc/sys/vm/drop_caches ]]; then
    sync && echo 3 > /proc/sys/vm/drop_caches
  fi
}

echo "writer,seal,compression,inputs,outputs,bytes,map_ms,register_ms,seal_ms,read_ms,total_ms,files,disk_mib,read_mib"

for WRITER in oneshot append multi_file; do
  for SEAL in never always; do
    for OUTPUTS in 200 512 2048 8192; do
      # multi_file at 8192 outputs × 100 inputs = 819k files — skip unless you've raised ulimit
      if [[ "$WRITER" == "multi_file" ]] && [[ "$OUTPUTS" -gt 2048 ]]; then continue; fi
      drop_caches
      DAFT_SHUFFLE_READ_PREFETCH=$PREFETCH "$BIN" \
        --writer "$WRITER" \
        --seal "$SEAL" \
        --inputs "$INPUTS" \
        --outputs "$OUTPUTS" \
        --bytes "$BYTES" \
        --payload-cols "$PAYLOAD_COLS" \
        --map-conc "$MAP_CONC" \
        --read-conc "$READ_CONC" \
        2>/dev/null | grep '^CSV:' | sed 's/^CSV: //'
    done
  done
done
