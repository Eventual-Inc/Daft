# Windows Path Fix Status

## Problem
PR #5819 fixes Windows path handling in `local_path_from_uri`, but Windows Rust tests only run on `main`, not PRs.

## Solution
Created test PR #5820 with Windows CI enabled to verify the fix.

## Changes Made (on `test-windows-ci-fix` branch)

1. **`src/daft-io/src/lib.rs`**: Added `strip_leading_slash_before_drive()` function (`#[cfg(windows)]` only)
   - Strips leading `/` from paths like `/C:/Users/...` → `C:/Users/...`
   - Docstring explains the purpose; no redundant inline comments needed

2. **`src/daft-io/src/local.rs`**: Added `#[cfg(windows)]` guarded calls at 4 locations where `file://` prefix is stripped

3. **`src/daft-scan/src/scan_task_iters/split_jsonl/mod.rs`**: Added `#[cfg(windows)]` guarded calls at 2 locations in `local_path_from_uri()`

4. **`.github/workflows/pr-test-suite.yml`**: Temporarily removed Windows exclusion (**must revert before merging**)

## Current Status
- Waiting for Windows CI on PR #5820 to pass
- Latest commit: `0c19dbd2d` (removed redundant comments)

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
