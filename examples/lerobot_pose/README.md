# LeRobot + H-RDT: per-frame 48-D action prediction with Daft
This example reads the [EgoDex test dataset](https://huggingface.co/datasets/pepijn223/egodex-test) (LeRobot v3 format) as a lazy Daft DataFrame — one row per frame, with the decoded camera image and the 48-D hand state — runs the [H-RDT](https://github.com/HongzheBi/H_RDT) policy on every frame, and stores the predicted 48-D action vector as a new column.

```
LeRobot v3 dataset (parquet + mp4 shards on HF Hub)
        │  lerobot.read(..., load_video_frames=True)        # lazy, streaming
        ▼
one row per frame: observation.image · observation.state (48) · task_index · ...
        │  HRDTPredictor.predict(...)                        # @daft.cls batch UDF on GPU
        ▼
+ predicted_action (48-D float vector)
        │  write_parquet
        ▼
out/egodex_hrdt_predictions/
```

## Files

| File | Purpose |
| --- | --- |
| `lerobot.py` | Vendored copy of `daft.datasets.lerobot` (LeRobot v3 reader). Delete once it ships in a Daft release. |
| `encode_task_embeddings.py` | One-time preprocessing: encode each task instruction with T5-XXL and cache it (H-RDT consumes language *embeddings*, not text). |
| `predict_poses.py` | The pipeline: decode frames → batched H-RDT inference → write parquet (pure predictions). |
| `compute_metrics.py` | Score the predictions: per-frame `avg_keypoint_distance_m` (EgoDex paper metric, arXiv:2505.11709 §4.3) + per-episode and overall summaries. Torch-free, re-runnable in seconds. |
| `visualize_predictions.py` | Project predicted vs ground-truth hand poses onto frames; writes PNG overlays and per-episode mp4s. |

## Setup

1. **Clone H_RDT** as a sibling of this repo (or set `HRDT_PROJECT_ROOT`):

   ```bash
   git clone https://github.com/HongzheBi/H_RDT ../../../H_RDT
   ```

2. **Download the pretrained weights** into the clone (~8.8 GB). This pulls the
   EgoDex pretrain checkpoint (`checkpoints/pretrain-0618/checkpoint-500000`,
   ~4.1 GB) and the DinoSigLIP vision backbone weights, skipping the duplicate
   safetensors copies the code doesn't load:

   ```bash
   uvx --from huggingface_hub hf download embodiedfoundation/H-RDT \
     --include "checkpoints/*" \
     --include "bak/dino-siglip/vit_large_patch14_reg4_dinov2.lvd142m/pytorch_model.bin" \
     --include "bak/dino-siglip/vit_large_patch14_reg4_dinov2.lvd142m/config.json" \
     --include "bak/dino-siglip/vit_so400m_patch14_siglip_384/open_clip_pytorch_model.bin" \
     --include "bak/dino-siglip/vit_so400m_patch14_siglip_384/*.json" \
     --local-dir ../../../H_RDT
   ```

3. **T5-XXL encoder** downloads automatically (~9.5 GB into the HF cache) the
   first time you run `encode_task_embeddings.py`. The default model is
   `city96/t5-v1_1-xxl-encoder-bf16`, an encoder-only bfloat16 conversion of
   `google/t5-v1_1-xxl` — numerically equivalent for our purposes (H-RDT's own
   pipeline ran the encoder in bfloat16) but 4.7x smaller than the official
   fp32 encoder+decoder repo. Loading takes ~10 GB RAM, CPU is fine. Set
   `T5_MODEL_PATH=google/t5-v1_1-xxl` to use the original instead (~44.5 GB).

## Run

```bash
# 1. One-time: cache a T5 embedding per task (3 tasks in egodex-test)
uv run encode_task_embeddings.py

# 2. Quick local trial: predict only the first 8 frames
MAX_FRAMES=8 uv run predict_poses.py

# 3. Full run: predict an action for every frame and write parquet
uv run predict_poses.py

# 4. Score the predictions (EgoDex paper's 12-keypoint metric) — re-runnable
uv run compute_metrics.py

# 5. Render overlay PNGs and per-episode mp4s of predicted vs ground truth
uv run visualize_predictions.py
```

## How it works

- **Lazy frames.** `lerobot.read` only builds a plan. Execution streams shard
  downloads, video decoding, inference, and writing — the full dataset never
  sits in memory.
- **`@daft.cls` for the model.** Daft constructs the predictor once per worker
  process (loading ~9 GB of weights in `__init__`) and reuses it for every
  batch. A plain function UDF would have nowhere to keep the loaded model.
- **Batched inference.** `@daft.method.batch(batch_size=16)` hands the UDF
  whole columns (`daft.Series`) at a time, so DinoSigLIP and the policy run one
  forward pass per 16 frames instead of per frame.
- **Concurrency.** Frame decoding (CPU) is fanned out across cores by Daft —
  the reader splits work with `into_batches(16)` — and overlaps with inference,
  which runs on a single model instance (`@daft.cls(gpus=..., max_concurrency=1)`).
  To run N concurrent model replicas on one GPU, use fractional GPUs:
  `@daft.cls(gpus=1/N, max_concurrency=N)` — each replica holds its own copy of
  the weights (~6.5 GB VRAM in bf16), so size VRAM accordingly.
- **Normalization contract.** Following H-RDT's EgoDex pretraining
  (`datasets/pretrain/egodex_dataset.py`): the input state is min/max scaled to
  `[-1, 1]` using `egodex_stat.json`, and the predicted chunk is denormalized
  with the inverse mapping. The model predicts 16 future steps; we keep step 0,
  so `predicted_action` is one 48-D vector per frame (24 dims per hand: wrist
  pose + finger keypoints).

## Output

`out/egodex_hrdt_predictions/` — parquet with one row per frame:
`episode_index`, `frame_index`, `timestamp`, `task_index`,
`observation.state`, `ground_truth_action`, `predicted_action` (48-D
`embedding` column), ready for `daft.read_parquet` to evaluate prediction error
against the ground-truth actions.

