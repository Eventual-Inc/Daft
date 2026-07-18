#!/bin/bash
# A/B driver for hand_tracking.py: original reader (pre-#7184) vs the current tree.
#
# Same approach as run_real_datasets.sh: the fix is Python-only, so swap
# daft/datasets/lerobot.py in the working tree between runs on one build.
# The original revision is pinned to the parent of the #7184 merge commit,
# since main now includes the fix. Requires daft imported from this repo,
# `daft-physical-ai[mediapipe]` installed, a clean lerobot.py, and network
# access to Hugging Face. Restores the file on exit.
#
#   ./run_hand_tracking.sh
set -u
cd "$(dirname "$0")/../.."
PY=${PY:-.venv/bin/python}
ORIG_REV=${ORIG_REV:-0a01463a2~1}  # parent of "feat(lerobot): batch video-frame decode by shard (#7184)"
OUT=$(dirname "$0")

echo "tree: $(git log -1 --format='%h %s')"
echo "original reader: $(git log -1 --format='%h %s' "$ORIG_REV")"
git diff --quiet -- daft/datasets/lerobot.py || { echo "lerobot.py has local changes; aborting"; exit 1; }
trap 'git checkout -- daft/datasets/lerobot.py; echo "[restored lerobot.py]"' EXIT

git show "$ORIG_REV:daft/datasets/lerobot.py" > daft/datasets/lerobot.py
"$PY" "$OUT/hand_tracking.py" "$OUT/hand_tracking_orig.json" || { echo "ORIG RUN FAILED"; exit 1; }
git checkout -- daft/datasets/lerobot.py
"$PY" "$OUT/hand_tracking.py" "$OUT/hand_tracking_batched.json" || { echo "BATCHED RUN FAILED"; exit 1; }
"$PY" "$OUT/hand_tracking.py" --chart "$OUT/hand_tracking_orig.json" "$OUT/hand_tracking_batched.json"
