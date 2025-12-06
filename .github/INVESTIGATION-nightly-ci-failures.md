# Investigation: Nightly CI Failures

**Date:** 2025-12-06
**Branch:** `investigate/nightly-ci-failures`
**Conversation ID:** `54cabd2d-711a-454b-a392-ef74e62aae9e`

## Summary

The nightly S3 publish workflow and TPC-H benchmark workflows have been failing consistently since **October 22, 2025** (last success).

## Root Cause

**DNS resolution failure during artifact upload on macOS x86_64 builds.**

The `macos-x86_64-lts=true` build job fails at the artifact upload step with:
```
##[error]Failed to CreateArtifact: Unable to make request: ENOTFOUND
```

### Why This Happens

1. **Runner Configuration Issue** (`.github/workflows/build-wheel.yml` line 39):
   ```yaml
   runs-on: ${{ inputs.os == 'ubuntu' && 'buildjet-16vcpu-ubuntu-2204' || format('{0}-latest', inputs.os) }}
   ```

2. For macOS, this resolves to `macos-latest`, which is now **macos-14 (ARM64/M1)** since April 30, 2024.

3. The build uses cross-compilation (`maturin target: x86_64`) and **succeeds** (55+ min compile time).

4. But artifact upload fails with network/DNS issues - likely infrastructure differences between ARM and Intel macOS runners.

## Evidence

- Last 50+ nightly runs: all failed or cancelled
- Last success: October 22, 2025
- All failures are the same job: `Build Daft wheel for macos-x86_64-lts=true / build`
- Build completes successfully, only upload fails

## Proposed Fixes

### Option 1: Use Intel Runner for x86_64 Builds (Recommended)
Change the runner selection to use `macos-13` (Intel) for x86_64 builds:

```yaml
runs-on: ${{
  inputs.os == 'ubuntu' && 'buildjet-16vcpu-ubuntu-2204' ||
  (inputs.os == 'macos' && inputs.arch == 'x86_64') && 'macos-13' ||
  format('{0}-latest', inputs.os)
}}
```

### Option 2: Add Retry Logic
Add retry logic to the upload-artifact step (but this may not help if it's a systematic issue).

### Option 3: Drop x86_64 macOS LTS Builds
If Intel macOS support isn't critical, consider removing this build matrix entry.

## Files to Modify

- `.github/workflows/build-wheel.yml` (line 39 - runner selection)

## References

- [GitHub: macos-latest migration to M1](https://github.blog/changelog/2024-01-30-github-actions-macos-14-sonoma-is-now-available/)
- [GitHub Actions workflow run #19983601918](https://github.com/Eventual-Inc/Daft/actions/runs/19983601918)
- Failed job: `Build Daft wheel for macos-x86_64-lts=true / build`

## Next Steps

1. Implement Option 1 (use `macos-13` for x86_64 builds)
2. Test by triggering a nightly build manually
3. Monitor for success

---
*Investigation by Claude Code - to resume, use the conversation ID above*
