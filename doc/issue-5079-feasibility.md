# Issue 5079 Feasibility Analysis

## 1. issue summary
- Issue: [scan csv does not know how much data is read](https://github.com/Eventual-Inc/Daft/issues/5079)
- Symptom: CSV scan execution does not report `bytes.read` progress stats correctly.
- Scope: native CSV local streaming read path.

## 2. root cause
- The local CSV reader (`src/daft-csv/src/local.rs`) reads data slab-by-slab from file descriptors.
- However, bytes read in this path were not marked into `IOStatsRef` during iteration.
- As a result, source snapshots/subscriber metrics could not reflect read-byte progress for CSV scans.

## 3. expected modification modules
- `src/daft-csv/src/local.rs`
  - Thread `IOStatsRef` into slab iterator path.
  - Mark `bytes.read` after each successful file read.
- `tests/test_subscribers.py`
  - Add regression test to assert CSV scan emits `bytes.read > 0` metrics.

## 4. implementation plan
1. Ensure `stream_csv_local` passes io stats through to streaming iterator path.
2. Replace unused local byte counter state with `Option<IOStatsRef>` in slab iterator.
3. On each slab read, call `io_stats.mark_bytes_read(bytes_read)` when available.
4. Add subscriber-level regression test for local CSV scan progress accounting.
5. Validate via documented workflow:
   - `make .venv`
   - `make build`
   - `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/test_subscribers.py -k csv_scan_reports_bytes_read"`

## Feasibility verdict
- Feasible and low-risk.
- Estimated touched files: 2 (<20).
- No API design change.
- No architecture adjustment.
- No multi-module refactor required.
