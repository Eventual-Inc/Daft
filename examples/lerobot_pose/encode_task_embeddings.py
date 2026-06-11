# /// script
# description = "Precompute T5 language embeddings for every task in a LeRobot dataset (H-RDT input)"
# requires-python = ">=3.12, <3.13"
# dependencies = [
#     "daft>=0.7.15",
#     "torch",
#     "transformers",
#     "sentencepiece",
#     "protobuf",
#     "accelerate",
# ]
# ///
"""Encode each task instruction in the dataset with T5 and cache it to disk.

H-RDT never sees raw text. At train and inference time it consumes language
*embeddings*: the task instruction run through a frozen T5 encoder
(`t5-v1_1-xxl`, 4096-dim features). Encoding is expensive (T5-XXL is an ~11B
parameter model), but a dataset only has a handful of distinct task strings,
so we encode each one exactly once up front and store it as a `.pt` file keyed
by `task_index`. The prediction pipeline then does a dictionary lookup per row
instead of running T5 per row.

This mirrors H_RDT's `models/encoder/t5_encoder.py` (same model, tokenizer
settings, and bfloat16 dtype) but calls `transformers` directly, because
`T5Embedder` hardcodes the author's local weight path in an assert. Only the
encoder half of T5 is loaded (~4.7B params, ~10 GB RAM in bfloat16), so this
runs fine on CPU.

Run this once before `predict_hrdt.py`:

    uv run encode_task_embeddings.py
"""

import os

import lerobot  # vendored copy of daft.datasets.lerobot
import torch

DATASET_URI = "pepijn223/egodex-test"
# HF id of (or local path to) the T5 model. Must be the XXL variant: H-RDT's
# text adapter expects 4096-dim features. The default is an encoder-only
# bfloat16 conversion of google/t5-v1_1-xxl (~9.5 GB download instead of the
# official repo's 44.5 GB fp32 encoder+decoder). It is numerically equivalent
# here: H-RDT's own T5Embedder loaded the encoder in bfloat16 too.
T5_MODEL_PATH = os.environ.get("T5_MODEL_PATH", "city96/t5-v1_1-xxl-encoder-bf16")
# Matches `tokenizer_max_length` in H_RDT's configs/hrdt_pretrain.yaml.
TOKENIZER_MAX_LENGTH = 1024
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "task_embeddings")


def load_tasks(dataset_uri: str) -> dict[int, str]:
    """Return {task_index: task instruction} from the dataset's `meta/tasks.parquet`."""
    df = lerobot.read_tasks(dataset_uri).collect()
    data = df.to_pydict()
    # The task string column name varies with how the dataset was exported
    # (e.g. pandas writes the task as the index column `__index_level_0__`),
    # so find it by dtype rather than by name.
    text_col = next(name for name, values in data.items() if values and isinstance(values[0], str))
    return dict(zip(data["task_index"], data[text_col]))


def main() -> None:
    from transformers import AutoTokenizer, T5EncoderModel

    tasks = load_tasks(DATASET_URI)
    print(f"Found {len(tasks)} tasks in {DATASET_URI}:")
    for idx, text in sorted(tasks.items()):
        print(f"  [{idx}] {text}")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    tokenizer = AutoTokenizer.from_pretrained(T5_MODEL_PATH, model_max_length=TOKENIZER_MAX_LENGTH)
    print(f"Loading T5 encoder on {device} (bfloat16)...")
    model = (
        T5EncoderModel.from_pretrained(
            T5_MODEL_PATH,
            torch_dtype=torch.bfloat16,
            low_cpu_mem_usage=True,  # stream + convert weights instead of loading all fp32 at once
        )
        .to(device)
        .eval()
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with torch.no_grad():
        for idx, text in sorted(tasks.items()):
            tokenized = tokenizer(
                [text],
                max_length=TOKENIZER_MAX_LENGTH,
                padding="longest",
                truncation=True,
                return_attention_mask=True,
                add_special_tokens=True,
                return_tensors="pt",
            )
            embeddings = model(
                input_ids=tokenized["input_ids"].to(device),
                attention_mask=tokenized["attention_mask"].to(device),
            )["last_hidden_state"]
            # Trim padding so we only store (and later attend over) real tokens.
            num_tokens = int(tokenized["attention_mask"][0].sum())
            trimmed = embeddings[0, :num_tokens].to(torch.float32).cpu()
            out_path = os.path.join(OUTPUT_DIR, f"task_{idx:03d}.pt")
            torch.save({"task_index": idx, "task": text, "embeddings": trimmed}, out_path)
            print(f"Saved {out_path} (shape {tuple(trimmed.shape)})")


if __name__ == "__main__":
    main()
