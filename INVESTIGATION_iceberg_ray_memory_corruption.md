# Investigation: Iceberg + Ray Integration Test Memory Corruption

**Status:** Open
**Priority:** Medium (Flaky test, ~10% failure rate)
**Date Started:** 2025-11-24
**Branch:** `investigate/iceberg-ray-memory-corruption`

---

## Executive Summary

The `integration-test-catalogs` job fails intermittently (~3 out of 10+ runs) with a memory corruption error (`free(): invalid next size (normal)`) in the Ray actor cleanup code. This happens specifically when running Iceberg integration tests with the Ray runner.

**Impact:** Blocks PRs randomly, unrelated to code changes
**Root Cause:** Likely a bug in Ray 2.48.0's ObjectRefGenerator cleanup when handling large PyArrow objects from Iceberg tables

---

## What We Know (Confirmed)

### 1. Failure Pattern is Non-Deterministic
Analysis of recent CI runs shows this is a **flaky test**, not caused by a specific commit:

| Date/Time | Run ID | Event | Catalogs Result | Overall |
|-----------|--------|-------|-----------------|---------|
| 2025-11-22 02:21 | [19588785156](https://github.com/Eventual-Inc/Daft/actions/runs/19588785156) | PR | ✅ success | ❌ fail (other) |
| 2025-11-22 07:28 | [19592288588](https://github.com/Eventual-Inc/Daft/actions/runs/19592288588) | PR | ❌ **FAILURE** | ❌ fail |
| 2025-11-22 10:46 | [19594398740](https://github.com/Eventual-Inc/Daft/actions/runs/19594398740) | PR | ✅ success | ❌ fail (other) |
| 2025-11-23 06:48 | [19607379379](https://github.com/Eventual-Inc/Daft/actions/runs/19607379379) | push (main) | ✅ success | ✅ success |
| 2025-11-23 07:30 | [19607836642](https://github.com/Eventual-Inc/Daft/actions/runs/19607836642) | PR | ✅ success | ❌ fail (other) |
| 2025-11-23 08:57 | [19608761410](https://github.com/Eventual-Inc/Daft/actions/runs/19608761410) | PR | ❌ **FAILURE** | ❌ fail |
| 2025-11-24 13:09 | [19635446865](https://github.com/Eventual-Inc/Daft/actions/runs/19635446865) | PR | ✅ success | ❌ fail (other) |
| 2025-11-24 17:40 | [19643652830](https://github.com/Eventual-Inc/Daft/actions/runs/19643652830) | PR | ✅ success | ✅ success |
| 2025-11-24 17:44 | [19643761235](https://github.com/Eventual-Inc/Daft/actions/runs/19643761235) | PR (#5650) | ❌ **FAILURE** | ❌ fail |

**Pattern:** Fails ~30% of the time, independent of code changes. Passes on main and most PRs.

### 2. Exact Error Signature

From [Run 19643761235 logs](https://github.com/Eventual-Inc/Daft/actions/runs/19643761235/job/56254846045):

```
[36m(RemoteFlotillaRunner pid=12058)[0m free(): invalid next size (normal)
[36m(RemoteFlotillaRunner pid=12058)[0m *** SIGABRT received at time=1764007559 on cpu 2 ***
[36m(RemoteFlotillaRunner pid=12058)[0m Fatal Python error: Aborted

ray.exceptions.ActorDiedError: The actor died unexpectedly before finishing this task.
	class_name: RemoteFlotillaRunner
	actor_id: 5665db62272c25784894d05301000000
	pid: 12058
	name: flotilla-plan-runner
```

**Stack trace shows cleanup path:**
```
ObjectRefGenerator.__del__()
→ async_delete_object_ref_stream()
  → ray::core::CoreWorker::AsyncDelObjectRefStream()
    → absl::raw_hash_set::prepare_insert()
      → cfree
        → free(): invalid next size (normal) 💥
```

### 3. Environment Details

**Versions (confirmed from CI logs):**
- Ray: 2.48.0
- PyArrow: 22.0.0
- Python: 3.10.19
- OS: Ubuntu (GitHub Actions runner)

**Test that triggers crash:**
- File: `tests/integration/iceberg/test_iceberg_reads.py:95`
- Function: `test_daft_iceberg_table_collect_correct()`
- Operation: `df.collect()` on Iceberg table read via Ray runner

### 4. Crash Happens During Cleanup

The error occurs in `ObjectRefGenerator.__del__()`, meaning it's during **garbage collection/cleanup**, not during actual data processing. The test likely succeeds functionally, but crashes when Ray tries to clean up object references.

### 5. No Recent Code Changes Correlate

Checked commits between 2025-11-15 and 2025-11-24:
- No Iceberg-specific changes that would cause this
- No Ray runner changes
- Dependency bump [ce0a97c43](https://github.com/Eventual-Inc/Daft/commit/ce0a97c4343b57942a6d4d573e51574fb26d5928) on 2025-11-21 was GitHub Actions versions, not Python packages

---

## What We Suspect (High Confidence)

### 1. Ray + PyArrow Memory Management Bug

**Evidence:**
- Ray has known memory issues with PyArrow objects:
  - [Ray #46366](https://github.com/ray-project/ray/issues/46366): Enormous memory usage with PyArrow Dataset in Ray worker (July 2024)
  - [Ray #49158](https://github.com/ray-project/ray/issues/49158): Memory leak reading large Parquet files (Dec 2024), traced to PyArrow leak
- Iceberg tables return large PyArrow Table objects that get serialized through Ray's object store

### 2. Race Condition in ObjectRefGenerator Cleanup

**Evidence:**
- Crash is in `absl::raw_hash_set::prepare_insert()` during cleanup
- Hash table corruption suggests concurrent access or use-after-free
- Flaky nature (30% failure rate) is characteristic of race conditions
- [Ray #28686](https://github.com/ray-project/ray/issues/28686): Known leak where dynamic generators don't clean up properly

### 3. Memory Pressure Triggers the Bug

**Why it's flaky:**
- Fast test execution → objects cleaned up in safe order → ✅ pass
- Slower execution or memory pressure → objects accumulate → cleanup order matters → ❌ crash

---

## What We Don't Know (Needs Investigation)

### 1. Is This Specific to Iceberg Tables?
**Unknown:** Does this happen with other large PyArrow data sources in Ray runner?

**To test:**
- Run similar tests with plain Parquet files (not Iceberg)
- Try with Delta Lake tables
- Test with synthetic large PyArrow tables

### 2. Does Ray 2.49+ or 2.50+ Fix This?
**Unknown:** Ray version 2.48.0 is current, but newer versions exist

**To check:**
- Ray 2.50.1 (latest as of Nov 2024): https://docs.ray.io/en/latest/
- Ray 2.52.0 release notes for relevant fixes
- Test with `ray>=2.50.0` locally

### 3. Does PyArrow 23+ Help?
**Unknown:** PyArrow 22.0.0 is current, but 23.0.0+ may have fixes

**To check:**
- PyArrow release notes: https://arrow.apache.org/release/
- Known memory leak fixes in recent versions
- Compatibility with Ray 2.48.0

### 4. Is Object Size a Factor?
**Unknown:** Does this only happen with tables over a certain size?

**To investigate:**
- Check sizes of Iceberg tables in failing tests
- Compare with non-Iceberg PyArrow data
- Look for patterns in which specific tables fail

### 5. Can We Reproduce Locally?
**Unknown:** Can we force this to fail consistently?

**To try:**
- Run `DAFT_RUNNER=ray pytest tests/integration/iceberg/test_iceberg_reads.py -v` repeatedly
- Add memory pressure (limit available RAM)
- Force GC during/after tests: `import gc; gc.collect()`

---

## Relevant Source Code Locations

### Daft Code
- **Test file:** `tests/integration/iceberg/test_iceberg_reads.py:91-98`
- **Ray runner:** `daft/runners/ray_runner.py:1413` (run method)
- **Flotilla runner:** `daft/runners/flotilla.py:324` (stream_plan method)

### Upstream Issues
- Ray ObjectRefGenerator leak: https://github.com/ray-project/ray/issues/28686
- Ray + PyArrow memory: https://github.com/ray-project/ray/issues/46366
- RayData Parquet leak: https://github.com/ray-project/ray/issues/49158
- PyIceberg memory leak: https://github.com/apache/iceberg-python/issues/1809
- PyArrow conversion leak: https://github.com/apache/arrow/issues/45504

---

## Potential Solutions (Untested)

### Short-term (Workarounds)
1. **Skip on Ray runner:** Mark problematic tests with `@pytest.mark.skipif(runner == "ray")`
2. **Explicit cleanup:** Add `ray.shutdown()` in test teardown
3. **Retry flaky tests:** Configure pytest to retry failed tests once

### Medium-term (Fixes)
1. **Upgrade Ray:** Test with Ray 2.50+ or 2.52+
2. **Upgrade PyArrow:** Test with PyArrow 23.0+
3. **Reduce object sizes:** Process Iceberg data in smaller chunks
4. **Manual GC:** Force `gc.collect()` between tests to control cleanup timing

### Long-term (Upstream)
1. **Report to Ray project:** File detailed bug report with reproduction steps
2. **Bisect Ray versions:** Find which Ray version introduced the regression
3. **Profile memory usage:** Use Ray memory profiler to identify leak source

---

## Next Steps for Investigation

1. **Reproduce locally:**
   ```bash
   DAFT_RUNNER=ray pytest tests/integration/iceberg/test_iceberg_reads.py::test_daft_iceberg_table_collect_correct -v --count=50
   ```

2. **Test with newer dependencies:**
   ```bash
   uv pip install 'ray>=2.50.0' 'pyarrow>=23.0.0'
   make test EXTRA_ARGS="-v tests/integration/iceberg/test_iceberg_reads.py"
   ```

3. **Add diagnostics:**
   - Instrument `flotilla.py` to log object ref counts
   - Add memory profiling to see when/where memory spikes
   - Log GC events during test execution

4. **Isolate the trigger:**
   - Run just the failing test parametrization
   - Test with native runner (should pass)
   - Gradually increase Iceberg table sizes to find threshold

5. **Check other PRs:**
   - Monitor if this persists in future runs
   - Track failure rate over time
   - See if specific test parameters fail more often

---

## Owner & Contact

**Investigating:** @yk
**Started:** 2025-11-24
**Related PR:** #5650 (triggered investigation but not root cause)

---

## References

- Original failed run: https://github.com/Eventual-Inc/Daft/actions/runs/19643761235
- PR that hit this: https://github.com/Eventual-Inc/Daft/pull/5650
- Ray documentation: https://docs.ray.io/en/latest/
- PyArrow memory docs: https://arrow.apache.org/docs/python/memory.html
