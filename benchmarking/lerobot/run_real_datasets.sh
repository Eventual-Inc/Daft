#!/bin/bash
# A/B driver for real_datasets.py: original reader (merge-base) vs this branch.
#
# Swaps daft/datasets/lerobot.py in the working tree between runs - the fix is
# Python-only, so the rest of the build is identical. Requires daft to be
# imported from this repo (make .venv / editable install), a clean lerobot.py,
# and network access to Hugging Face. Restores the file on exit.
#
#   ./run_real_datasets.sh [n_rows=16]
set -u
cd "$(dirname "$0")/../.."
PY=${PY:-.venv/bin/python}
N_ROWS=${1:-16}
BASE=$(git merge-base HEAD main)
OUT=$(dirname "$0")

echo "branch: $(git log -1 --format='%h %s')"
echo "merge-base (original): $(git log -1 --format='%h %s' "$BASE")"
git diff --quiet -- daft/datasets/lerobot.py || { echo "lerobot.py has local changes; aborting"; exit 1; }
trap 'git checkout -- daft/datasets/lerobot.py; echo "[restored lerobot.py]"' EXIT

DATASETS=(
  "AlexFeng1/fa_putPlace_35"
  "Cache-SCA/IsaacLab-SO101-Phase1-push_button-80episode"
  "Helloworldali/pick-cube-maniskill-dataset"
  "HCIS-Lab/soarm101-feeding-nuts"
  "Jackie1/bridge_data_v2_convert_to_lerobot"
  "DAVIAN-Robotics/robocasa-MG_3000"
)

for ds in "${DATASETS[@]}"; do
  slug=$(echo "$ds" | tr '/-' '__')
  echo "===== $ds ====="
  git show "$BASE:daft/datasets/lerobot.py" > daft/datasets/lerobot.py
  "$PY" "$OUT/real_datasets.py" "$ds" "$OUT/ab_${slug}_orig.json" "$N_ROWS" || { echo "ORIG RUN FAILED: $ds"; git checkout -- daft/datasets/lerobot.py; continue; }
  git checkout -- daft/datasets/lerobot.py
  "$PY" "$OUT/real_datasets.py" "$ds" "$OUT/ab_${slug}_batched.json" "$N_ROWS" || { echo "BATCHED RUN FAILED: $ds"; continue; }
  "$PY" "$OUT/real_datasets.py" --compare "$OUT/ab_${slug}_orig.json" "$OUT/ab_${slug}_batched.json"
done
echo "===== ALL DONE ====="
