# /// script
# description = "Run H-RDT action prediction over a LeRobot dataset with Daft"
# requires-python = ">=3.12, <3.13"
# dependencies = [
#     "daft>=0.7.15",
#     "torch",
#     "torchvision",
#     "timm",
#     "transformers",
#     "sentencepiece",
#     "diffusers",
#     "huggingface-hub",
#     "av",
#     "pillow",
#     "opencv-python",
#     "pyyaml",
#     "numpy",
# ]
# ///
"""Predict 48-D hand actions for every frame of an EgoDex LeRobot dataset.

The pipeline is as follows: 1) we load in a lazy Daft DataFrame
with 1 row per frame. Each row contains the decoded camera image, the 
48-D observation state, and the episode/task metadata. 2) we wrap the 
H-RDT model as a Daft class, which allows for a persistent state in which 
each worker builds the model once in __init__ and reuses it for every batch. 
3) we append the predicted step per row as a new column in the DataFrame. 
4) we write the DataFrame to a parquet file. 

The model predicts a chunk of 16 future actions. However we keep only the first step of each chunk,
so the new column is a single 48-D float vector per frame.

Run `encode_task_embeddings.py` first to cache the T5 task embeddings, then:

    uv run predict_poses.py
"""

# ruff: noqa: E402  -- sys.path must be extended before the `models.*` imports below
import os
import sys

# Let any operator MPS doesn't implement fall back to CPU instead of crashing.
# Must be set before torch initializes the MPS backend.
os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")

HRDT_ROOT = os.environ.get(
    "HRDT_PROJECT_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../H_RDT")),
) # the HRDT model repo must be cloned either in the directory specified by env or at the sibling level of the current repo
sys.path.append(HRDT_ROOT)

import json

import lerobot  # vendored copy of daft.datasets.lerobot
import numpy as np
import torch
import yaml
from PIL import Image as PILImage

# NOTE: the H_RDT imports (`from models...`) deliberately live inside
# HRDTPredictor.__init__, NOT here. Daft pickles the class to ship it to worker
# processes; pip-installed packages pickle by reference, but H_RDT is only
# importable via the sys.path hack above, so pickle tries to serialize its
# classes by value and fails ("cannot pickle 'GenericModule' object").
# Importing inside __init__ defers the import to the worker process instead.
import daft
from daft import DataType, col

DATASET_URI = "pepijn223/egodex-test"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "out", "egodex_hrdt_predictions")
TASK_EMBEDDINGS_DIR = os.path.join(os.path.dirname(__file__), "task_embeddings")

# The EgoDex-pretrained checkpoint published in the H-RDT model repo.
CHECKPOINT_PATH = os.path.join(HRDT_ROOT, "checkpoints", "pretrain-0618", "checkpoint-500000")
CONFIG_PATH = os.path.join(HRDT_ROOT, "configs", "hrdt_pretrain.yaml")
STAT_PATH = os.path.join(HRDT_ROOT, "datasets", "pretrain", "egodex_stat.json")

HAS_GPU = torch.cuda.is_available()
PREDICT_BATCH_SIZE = 16
# For a quick local trial, set MAX_FRAMES to only predict the first N frames
# (e.g. `MAX_FRAMES=8 uv run predict_hrdt.py`). 0 means the whole dataset.
MAX_FRAMES = int(os.environ.get("MAX_FRAMES", "0"))
NUM_GPUS = 1 if HAS_GPU else 0

# Resolve the torch device HERE, at module level, not inside the class:
# `torch.backends.mps` is a property-object that cloudpickle cannot serialize,
# and Daft pickles the class (including everything its methods reference) to
# ship it to workers. A plain string global pickles fine.
if HAS_GPU:
    DEVICE = "cuda"
elif torch.backends.mps.is_available():
    DEVICE = "mps"  # Apple-silicon GPU via Metal
else:
    DEVICE = "cpu"

@daft.cls(gpus=NUM_GPUS, max_concurrency=1)
class HRDTPredictor:
    def __init__(self, ckpt_path: str, config_path: str, stat_path: str, embeddings_dir: str):
        # Runs once per replica, on the worker, when execution starts.
        # H_RDT imports happen here so they're never pickled (see note at top).
        from models.encoder.dinosiglip_vit import DinoSigLIPViTBackbone  # from H_RDT
        from models.hrdt_runner import HRDTRunner  # from H_RDT

        self.device = torch.device(DEVICE)
        print(f"Using device: {self.device}")

        if self.device.type == "cuda":
            self.dtype = torch.bfloat16
        else:
            # float32 on MPS and CPU: bfloat16 support on MPS is still patchy.
            self.dtype = torch.float32

        # task_index -> (num_tokens, 4096) T5 embedding, cached by
        # encode_task_embeddings.py. Loaded FIRST so a missing/empty cache fails
        # in milliseconds, before the minutes-long model weight loading below.
        self.lang_embeddings: dict[int, torch.Tensor] = {}
        try:
            fnames = sorted(os.listdir(embeddings_dir))
        except FileNotFoundError:
            fnames = []  # missing dir is handled the same as an empty one, below
        for fname in fnames:
            if fname.endswith(".pt"):
                payload = torch.load(os.path.join(embeddings_dir, fname), map_location="cpu")
                self.lang_embeddings[int(payload["task_index"])] = payload["embeddings"].to(
                    self.device, dtype=self.dtype
                )
        if not self.lang_embeddings:
            raise FileNotFoundError(
                f"No task embeddings found in {embeddings_dir}. Run encode_task_embeddings.py first."
            )

        with open(config_path) as f:
            config = yaml.safe_load(f)
       
        
        self.vision_encoder = DinoSigLIPViTBackbone(
            vision_backbone_id="dino-siglip",
            image_resize_strategy="letterbox"
            if config["dataset"]["image_aspect_ratio"] == "pad"
            else "resize-naive",
            default_image_size=384,
        )
        self.vision_encoder.to(self.device, dtype=self.dtype).eval()
        self.image_transform = self.vision_encoder.get_image_transform()

        common = config["common"]
        self.pred_horizon = common["action_chunk_size"]
        self.policy = HRDTRunner(
            state_dim=common["state_dim"],
            action_dim=common["action_dim"],
            pred_horizon=self.pred_horizon,
            config=config["model"],
            act_pos_emb_config=[
                ("state", 1),
                ("action", self.pred_horizon),
            ],
            img_pos_emb_config=[
                ("image", (common["img_history_size"], common["num_cameras"], -self.vision_encoder.num_patches)),
            ],
            lang_pos_emb_config=[
                ("language", -config["dataset"]["tokenizer_max_length"]),
            ],
            max_img_len=common["img_history_size"] * common["num_cameras"] * self.vision_encoder.num_patches,
            max_lang_len=config["dataset"]["tokenizer_max_length"],
            training_mode="lang",
            mode="pretrain",
            dtype=self.dtype,
        )
        state_dict = torch.load(
            os.path.join(ckpt_path, "pytorch_model.bin"), map_location="cpu", weights_only=True
        )
        
        state_dict = {k: v for k, v in state_dict.items() if not k.startswith("video_adapter.")}
        self.policy.load_state_dict(state_dict)
        self.policy.to(self.device, dtype=self.dtype).eval()

        with open(stat_path) as f:
            stat = json.load(f)["egodex"]
        self.action_min = np.array(stat["min"], dtype=np.float32)
        self.action_max = np.array(stat["max"], dtype=np.float32)

    @daft.method.batch(
        return_dtype=DataType.embedding(DataType.float32(), 48),
        batch_size=PREDICT_BATCH_SIZE,
    )
    def predict(self, images: daft.Series, states: daft.Series, task_indices: daft.Series):
        """Predict the next 48-D action for a batch of frames.

        Batch methods receive whole columns as `daft.Series` so we can run the
        vision encoder and the policy once per batch instead of once per row.
        """
        image_arrays = images.to_pylist()  # list of HWC uint8 numpy arrays
        state_batch = np.asarray(states.to_pylist(), dtype=np.float32)  # (B, 48)
        task_batch = task_indices.to_pylist()
        batch_size = len(image_arrays)

        with torch.no_grad():
            # State: normalize to [-1, 1] exactly like pretraining did.
            normalized = (state_batch - self.action_min) / (self.action_max - self.action_min) * 2 - 1
            state_tokens = (
                torch.from_numpy(np.clip(normalized, -1, 1))
                .reshape(batch_size, 1, -1)
                .to(self.device, dtype=self.dtype)
            )

            # Images: letterbox + normalize each frame, then encode the whole
            # batch in one DinoSigLIP forward pass.
            transformed = [self.image_transform(PILImage.fromarray(arr)) for arr in image_arrays]
            image_inputs = {
                key: torch.stack([t[key] for t in transformed]).to(self.device, dtype=self.dtype)
                for key in transformed[0]
            }
            image_features = self.vision_encoder(image_inputs)  # (B, num_patches, embed_dim)
            image_tokens = image_features.view(batch_size, -1, self.vision_encoder.embed_dim)

            # Language: look up each row's cached T5 embedding and pad to the
            # longest one in the batch, with an attention mask marking padding.
            embeds = [self.lang_embeddings[int(idx)] for idx in task_batch]
            max_len = max(e.shape[0] for e in embeds)
            lang_tokens = torch.zeros(batch_size, max_len, embeds[0].shape[1], device=self.device, dtype=self.dtype)
            lang_attn_mask = torch.zeros(batch_size, max_len, device=self.device, dtype=torch.bool)
            for i, e in enumerate(embeds):
                lang_tokens[i, : e.shape[0]] = e
                lang_attn_mask[i, : e.shape[0]] = True

            action_pred = self.policy.predict_action(
                state_tokens=state_tokens,
                image_tokens=image_tokens,
                lang_tokens=lang_tokens,
                lang_attn_mask=lang_attn_mask,
            )  # (B, pred_horizon, 48), normalized to [-1, 1]

            chunk = action_pred.float().cpu().numpy()
            # Denormalize (inverse of the [-1, 1] scaling) and keep only the
            # first step of each predicted 16-step chunk.
            denorm = (chunk + 1) / 2 * (self.action_max - self.action_min) + self.action_min
            return [row for row in denorm[:, 0, :].astype(np.float32)]


if __name__ == "__main__":
    predictor = HRDTPredictor(CHECKPOINT_PATH, CONFIG_PATH, STAT_PATH, TASK_EMBEDDINGS_DIR)

    df = lerobot.read(DATASET_URI, load_video_frames=True)
    if MAX_FRAMES:
        df = df.limit(MAX_FRAMES)

    df = (
        df.with_column(
            "predicted_action",
            predictor.predict(col("observation.image"), col("observation.state"), col("task_index")),
        )
        # Keep the trajectory data and identifiers; drop the decoded frames so the
        # output stays small (the images are reproducible from the dataset anyway).
        .select(
            "episode_index",
            "frame_index",
            "timestamp",
            "task_index",
            col("observation.state"),
            col("action").alias("ground_truth_action"),
            "predicted_action",
        )
    )

    df.write_parquet(OUTPUT_DIR)
    print(f"Wrote predictions to {OUTPUT_DIR}")
    print("Score them with: uv run compute_metrics.py")

    daft.read_parquet(f"{OUTPUT_DIR}/**").show(8)