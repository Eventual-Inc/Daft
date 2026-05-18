"""Live-aggregate oneshot_writer::profile lines from a streaming log.

Reads from --log; tails forever (or until EOF if --once). Maintains running
counts + stats per timing field. Prints a refreshed table every --interval
seconds (default 30) and a final summary on Ctrl-C.

Each profile line emitted by oneshot_writer.rs looks like (whitespace simplified):

  ... INFO daft_shuffles::oneshot_writer::profile: oneshot map task profile
    input_id=3 shuffle_id=2 num_partitions=2048 num_nonempty=2048
    num_arrow_batches=2048 total_rows=5999995 total_input_bytes=1160689592
    total_written_bytes=1172803336 compressed=false
    total_ms="1750.77" mp_concat_ms="12.33" arrow_concat_ms="378.84"
    to_arrow_ms="11.11" ipc_write_ms="492.63" ipc_finish_ms="0.00"
    other_ms="855.86"
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from statistics import mean, median

TIMING_FIELDS = [
    "total_ms",
    "concat_one_total_ms",
    "input_rollup_ms",
    "mp_concat_ms",
    "arrow_concat_ms",
    "to_arrow_ms",
    "concat_one_unattributed_ms",
    "ipc_write_ms",
    "ipc_finish_ms",
    "pcache_build_ms",
    "outer_loop_ms",
    "other_ms",
]
COUNT_FIELDS = [
    "num_partitions",
    "num_nonempty",
    "num_arrow_batches",
    "num_input_mps",
    "num_input_rbs",
    "total_rows",
    "total_input_bytes",
    "total_written_bytes",
]

# Fields are emitted by `tracing` as italic name + "=" + value or quoted-value.
# Strip ANSI first, then match "name=\"...\"" or "name=number" patterns.
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
FIELD_RE = re.compile(r"([a-z_]+)=(?:\"([^\"]*)\"|([^\s]+))")


def parse_profile(raw: str) -> dict | None:
    line = ANSI_RE.sub("", raw)
    if "oneshot map task profile" not in line:
        return None
    fields = {}
    for m in FIELD_RE.finditer(line):
        name = m.group(1)
        val = m.group(2) if m.group(2) is not None else m.group(3)
        fields[name] = val
    if "total_ms" not in fields:
        return None
    out = {"_raw": raw}
    for f in TIMING_FIELDS:
        try:
            out[f] = float(fields[f])
        except (KeyError, ValueError):
            out[f] = None
    for f in COUNT_FIELDS:
        try:
            out[f] = int(fields[f])
        except (KeyError, ValueError):
            out[f] = None
    out["compressed"] = fields.get("compressed")
    return out


def fmt_summary(profiles: list[dict]) -> str:
    n = len(profiles)
    if n == 0:
        return "no profile lines yet"
    lines = [f"=== profile aggregate over {n} map tasks (ts={time.strftime('%H:%M:%S')}) ==="]

    total_means = {}
    for f in TIMING_FIELDS:
        vals = [p[f] for p in profiles if p.get(f) is not None]
        total_means[f] = mean(vals) if vals else None
    base = total_means.get("total_ms") or 1.0
    pct_of_total = lambda x: 100 * x / base
    lines.append(f"{'field':<30} {'mean_ms':>10} {'median_ms':>10} {'sum_s':>10} {'min_ms':>10} {'max_ms':>10}  % of total")
    for f in TIMING_FIELDS:
        vals = [p[f] for p in profiles if p.get(f) is not None]
        if not vals:
            continue
        m, md = mean(vals), median(vals)
        s = sum(vals) / 1000.0
        lines.append(
            f"{f:<30} {m:>10.2f} {md:>10.2f} {s:>10.2f} {min(vals):>10.2f} {max(vals):>10.2f}  {pct_of_total(m):>6.1f}%"
        )

    def _sum(field):
        return sum(p.get(field, 0) or 0 for p in profiles)
    parts_total = _sum("num_partitions")
    batches = _sum("num_arrow_batches")
    mps = _sum("num_input_mps")
    in_rbs = _sum("num_input_rbs")
    rows = _sum("total_rows")
    in_bytes = _sum("total_input_bytes")
    wr_bytes = _sum("total_written_bytes")
    cs = [p["compressed"] for p in profiles if p.get("compressed") is not None]
    lines.append("")
    lines.append(f"counts:  parts_summed={parts_total}  arrow_batches={batches}")
    if mps:
        lines.append(f"         num_input_mps={mps}  num_input_rbs={in_rbs}  rbs/task={in_rbs/n:,.0f}")
    lines.append(f"         rows={rows:,}  input_bytes={in_bytes:,}  written_bytes={wr_bytes:,}")
    lines.append(f"         compressed={set(cs)}")
    if batches:
        lines.append(f"         rows/batch={rows/batches:,.1f}")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--interval", type=float, default=30.0)
    ap.add_argument("--once", action="store_true", help="parse current log content and exit")
    args = ap.parse_args()

    profiles: list[dict] = []
    seen_lines = 0

    def parse_more(fh, profiles):
        new = 0
        for line in fh:
            p = parse_profile(line)
            if p:
                profiles.append(p)
                new += 1
        return new

    fh = open(args.log, "r", encoding="utf-8", errors="replace")
    next_print = time.time() + args.interval
    try:
        while True:
            added = parse_more(fh, profiles)
            seen_lines += added
            now = time.time()
            if now >= next_print or args.once:
                print(fmt_summary(profiles), flush=True)
                next_print = now + args.interval
            if args.once:
                break
            time.sleep(2.0)
    except KeyboardInterrupt:
        pass

    print("--- FINAL ---")
    print(fmt_summary(profiles))


if __name__ == "__main__":
    main()
