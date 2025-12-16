# Windows Path Fix Status

## Problem
PR #5819 fixes Windows path handling in `local_path_from_uri`, but Windows Rust tests only run on `main`, not PRs.

## Solution
Created test PR #5820 with Windows CI enabled to verify the fix.

## Changes Made (on `test-windows-ci-fix` branch)

1. **`src/daft-io/src/lib.rs`**: Added `strip_file_uri_to_path()` helper function
   - Strips `file://` prefix and handles Windows drive letter paths in one call
   - Internal `strip_leading_slash_before_drive()` is now private

2. **`src/daft-io/src/local.rs`**: Uses `strip_file_uri_to_path()` at 4 call sites (simplified, no `#[cfg(windows)]` needed at call sites)

3. **`src/daft-scan/src/scan_task_iters/split_jsonl/mod.rs`**: Uses `strip_file_uri_to_path()` in `local_path_from_uri()`

4. **`.github/workflows/pr-test-suite.yml`**: Temporarily removed Windows exclusion (**must revert before merging**)

## Current Status
- Waiting for Windows CI on PR #5820 to pass
- Latest commit: `f58607d51` (consolidated into strip_file_uri_to_path helper)

## Next Steps

1. **Wait for CI** (Windows Rust tests take ~40 min):
   ```bash
   gh pr view 5820 --repo Eventual-Inc/Daft --json statusCheckRollup --jq '.statusCheckRollup[] | select(.name == "rust-tests-platform (Windows)") | {name, status, conclusion}'
   ```

2. **If CI passes**, apply fixes to original PR:
   ```bash
   # Switch to original branch
   git checkout fix-windows-jsonl-path

   # Cherry-pick fix commits (NOT the workflow change commit)
   git log --oneline test-windows-ci-fix  # Find relevant commits
   # Cherry-pick: daft-io fix, daft-scan fix, refactor commits

   git push  # Update PR #5819
   ```

3. **Cleanup**:
   ```bash
   gh pr close 5820 --repo Eventual-Inc/Daft
   git branch -D test-windows-ci-fix
   git push origin --delete test-windows-ci-fix
   ```

## Relevant PRs
- Original: https://github.com/Eventual-Inc/Daft/pull/5819
- Test: https://github.com/Eventual-Inc/Daft/pull/5820
