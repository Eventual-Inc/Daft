from __future__ import annotations

import io

import numpy as np
import ray
import torch
import torchaudio
import torchaudio.transforms as T
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor

TRANSCRIPTION_MODEL = "openai/whisper-tiny"
NUM_GPUS = 8
SAMPLING_RATE = 16000
INPUT_PATH = "s3://daft-public-datasets/common_voice_17"
OUTPUT_PATH = "s3://eventual-dev-benchmarking-results/ai-benchmark-results/audio-transcription"
BATCH_SIZE = 64

### This is a workaround to avoid the error:
### Casting from 'extension<ray.data.arrow_tensor_v2<ArrowTensorTypeV2>>' to different extension type 'extension<ray.data.arrow_variable_shaped_tensor<ArrowVariableShapedTensorType>>' not permitted.
### One can first cast to the storage type, then to the extension type.
def unnest(item):
    for k, v in item["audio"].items():
        item[k] = v
    del item["audio"]
    return item


def resample(item):
    audio_bytes = item["bytes"]
    waveform, sampling_rate = torchaudio.load(io.BytesIO(audio_bytes), format="flac")
    waveform = T.Resample(sampling_rate, SAMPLING_RATE)(waveform).squeeze()
    item["arr"] = waveform.tolist()
    return item


processor = AutoProcessor.from_pretrained(TRANSCRIPTION_MODEL)


def whisper_preprocess(batch):
    extracted_features = processor(
        batch["arr"].tolist(),
        sampling_rate=SAMPLING_RATE,
        return_tensors="np",
        device="cpu",
    ).input_features
    batch["input_features"] = [arr for arr in extracted_features]
    return batch


class Transcriber:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.dtype = torch.float16
        self.model_id = TRANSCRIPTION_MODEL
        self.model = AutoModelForSpeechSeq2Seq.from_pretrained(
            self.model_id,
            torch_dtype=self.dtype,
            low_cpu_mem_usage=True,
            use_safetensors=True,
        )
        self.model.to(self.device)

    def __call__(self, batch):
        spectrograms = np.array(batch["input_features"])
        spectrograms = torch.tensor(spectrograms).to(self.device, dtype=self.dtype)
        with torch.no_grad():
            token_ids = self.model.generate(spectrograms)
        batch["token_ids"] = token_ids.cpu().numpy()
        return batch


def decoder(batch):
    transcription = processor.batch_decode(batch["token_ids"], skip_special_tokens=True)
    batch["transcription"] = transcription
    batch["transcription_length"] = [len(t) for t in transcription]
    return batch


ds = ray.data.read_parquet(INPUT_PATH)
ds = ds.map(unnest)
ds = ds.map(resample)
ds = ds.map_batches(whisper_preprocess)
ds = ds.map_batches(
    Transcriber,
    batch_size=BATCH_SIZE,
    concurrency=NUM_GPUS,
    num_gpus=1,
)
ds = ds.map_batches(decoder)
ds = ds.drop_columns(["input_features", "token_ids", "arr"])
ds.write_parquet(OUTPUT_PATH)
