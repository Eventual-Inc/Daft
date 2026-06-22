# Contribution Progress: Issue 5690

Status: Phase III Complete

Branch: https://github.com/johnquevedo/Daft/tree/issue-5690-ray-sample-by-size

Issue: https://github.com/Eventual-Inc/Daft/issues/5690

## Summary

Implemented exact-size sampling for the distributed Ray runner. The Ray path now materializes distributed child outputs, feeds all partition refs into one final local `Sample` operation, and reuses Daft's existing local exact-size sampling algorithm. Fraction sampling is unchanged and still runs partition-locally.

This avoids the bias risk from taking equal-size local candidates from unequal partitions before a final sample. Because the final local sample sees every row in one stream, sampling without replacement and with replacement both use the same global algorithm as the native runner.

## Week 3 Progress

- Rebased the issue branch onto `upstream/main`.
- Verified the implementation patterns for distributed sample, gather, sort sampling, materialization, and local `SampleSink`.
- Removed the Ray-only API rejection for `DataFrame.sample(size=...)`.
- Added a distributed size-sampling path in `SampleNode`.
- Enabled the existing exact-size sampling tests for Ray.
- Added a deterministic uneven-partition regression test with an empty partition.
- Re-ran the original reproduction script and confirmed it prints `rows: 10`.
- Ran formatting, linting, pre-commit hooks, Python sample tests for native and Ray, and focused Rust package tests.

## Files Modified

- `src/daft-distributed/src/pipeline_node/sample.rs`
  - Added a blocking distributed execution path for `SamplingMethod::Size`.
  - Materializes child outputs in deterministic task order and builds a single in-memory local sample task.
  - Marks size sampling as a blocking sink with one unknown output partition.
  - Leaves fraction sampling on the existing per-partition pipeline instruction path.
- `daft/dataframe/dataframe.py`
  - Removed the Ray-only rejection for sample by size.
  - Removed the obsolete native-only note from the public API docstring.
- `tests/dataframe/test_sample.py`
  - Removed Ray skip markers from existing exact-size sampling tests.
  - Added `test_sample_size_unequal_partitions` to cover uneven partition sizes and an empty partition without probabilistic assertions.
- `CONTRIBUTION_README.md`
  - Added this Phase III contribution record.

## Key Commits

- `c439b2d44` - Reproduction evidence for issue 5690, rebased from original `3f707accd`.
- `0f19698cc` - Add Ray support for sample by size.
- Documentation update commit - Records Phase III implementation progress and validation.

## Implementation Decisions

- Used one final local `Sample` operation over all materialized distributed outputs for size sampling.
- Reused existing local `SampleSink` behavior instead of duplicating sampling logic.
- Kept fraction sampling unchanged because it is already partition-local by definition.
- Reported size sampling output clustering as one unknown partition because the final exact-size sample emits one local output partition.
- Preserved deterministic seeded behavior as much as the existing execution model supports by relying on `materialize_all_pipeline_outputs`, which uses `OrderedJoinSet` to emit materialized outputs in task submission order.

## Challenges

- A naive two-stage candidate strategy can bias samples when partitions have unequal sizes. The final implementation avoids that by sampling from the complete gathered stream.
- Ray startup and build hooks require process, cache, and network access that the sandbox blocks. Commands that failed for those reasons were rerun with approval.
- `make precommit` and `cargo test -p daft-distributed` initially failed in the sandbox due to the dashboard frontend build attempting to create a process and bind a port. Approved reruns completed successfully.

## Tests Added Or Enabled

- Enabled existing exact-size `tests/dataframe/test_sample.py` cases under Ray:
  - requested row count
  - size zero
  - oversized sample without replacement error
  - oversized sample with replacement
  - empty input behavior
  - multiple partitions
  - seeded output consistency
  - uniqueness without replacement
  - duplicates with replacement
- Added `test_sample_size_unequal_partitions`.
- Existing fraction sampling tests continue to run under Ray.

## Validation Commands

- `git status --short` - passed, initially clean before work.
- `git branch --show-current` - passed, branch was `issue-5690-ray-sample-by-size`.
- `git remote -v` - passed, `origin` was `johnquevedo/Daft` and `upstream` was `Eventual-Inc/Daft`.
- `git fetch upstream` - initial sandbox failure, approved rerun passed.
- `git rebase upstream/main` - passed.
- `make format` - first run reformatted Rust and exited nonzero, second run passed.
- `make build` - initial sandbox cache failure, approved rerun passed.
- `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/dataframe/test_sample.py"` - initial sandbox cache failure, approved rerun passed with 236 passed.
- `DAFT_RUNNER=ray make test EXTRA_ARGS="-v tests/dataframe/test_sample.py"` - passed with 236 passed.
- `.venv/bin/python reproductions/issue_5690.py` - initial sandbox process-inspection failure, approved rerun passed and printed `rows: 10`.
- `make check-format` - initial sandbox cache/network failure after Ruff fixed import ordering, approved rerun passed.
- `make lint` - passed.
- `make precommit` - initial sandbox cache/dashboard build failure, approved rerun passed.
- `cargo test -p daft-distributed` - initial sandbox dashboard build failure, approved rerun passed with 68 passed, 1 ignored.

## Technical Learnings

- Distributed `pipeline_instruction` is appropriate for local per-task operations, but exact global operations need a blocking stage over materialized outputs.
- `MaterializedOutput::into_in_memory_scan_with_psets_and_phase` is the existing pattern for building final local tasks from distributed outputs.
- `SampleSink` already implements the global exact-size algorithm needed for this issue.
- Equal local candidate counts are not a correct uniform strategy for unequal partition sizes.

## AI Disclosure

I used Codex to help navigate the codebase, draft implementation changes, and suggest tests. I reviewed each change, traced the relevant execution paths, ran the listed formatting and test commands locally, and verified the final behavior with the reproduction script.
