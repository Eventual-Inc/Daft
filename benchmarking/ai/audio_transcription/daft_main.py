from __future__ import annotations

import io

import numpy as np
import torch
import torchaudio
import torchaudio.transforms as T
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor
import time
import ray

import daft

TRANSCRIPTION_MODEL = "openai/whisper-tiny"
NUM_GPUS = 8
NEW_SAMPLING_RATE = 16000
INPUT_PATH = "s3://daft-public-datasets/common_voice_17"
OUTPUT_PATH = "s3://eventual-dev-benchmarking-results/ai-benchmark-results/audio-transcription"

daft.context.set_runner_ray()

# Wait for Ray cluster to be ready
@ray.remote
def warmup():
    pass
ray.get([warmup.remote() for _ in range(64)])


def resample(audio_bytes):
    waveform, sampling_rate = torchaudio.load(io.BytesIO(audio_bytes), format="flac")
    waveform = T.Resample(sampling_rate, NEW_SAMPLING_RATE)(waveform).squeeze()
    return np.array(waveform)


processor = AutoProcessor.from_pretrained(TRANSCRIPTION_MODEL)


@daft.func.batch(return_dtype=daft.DataType.tensor(daft.DataType.float32()))
def whisper_preprocess(resampled):
    extracted_features = processor(
        resampled.to_arrow().to_numpy(zero_copy_only=False).tolist(),
        sampling_rate=NEW_SAMPLING_RATE,
        device="cpu",
    ).input_features
    return extracted_features


@daft.cls(max_concurrency=NUM_GPUS, gpus=1)
class Transcriber:
    def __init__(self) -> None:
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.dtype = torch.float16
        self.model = AutoModelForSpeechSeq2Seq.from_pretrained(
            TRANSCRIPTION_MODEL,
            torch_dtype=self.dtype,
            low_cpu_mem_usage=True,
            use_safetensors=True,
        )
        self.model.to(self.device)

    @daft.method.batch(
        return_dtype=daft.DataType.list(daft.DataType.int32()),
        batch_size=64,
    )
    def __call__(self, extracted_features):
        spectrograms = np.array(extracted_features)
        spectrograms = torch.tensor(spectrograms).to(self.device, dtype=self.dtype)
        with torch.no_grad():
            token_ids = self.model.generate(spectrograms)

        return token_ids.cpu().numpy()


@daft.func.batch(return_dtype=daft.DataType.string())
def decoder(token_ids):
    transcription = processor.batch_decode(token_ids, skip_special_tokens=True)
    return transcription

daft.set_planning_config(default_io_config=daft.io.IOConfig(s3=daft.io.S3Config.from_env()))

start_time = time.time()

df = daft.read_parquet(INPUT_PATH)
df = df.with_column(
    "resampled",
    df["audio"]["bytes"].apply(resample, return_dtype=daft.DataType.list(daft.DataType.float32())),
)
df = df.with_column("extracted_features", whisper_preprocess(df["resampled"]))
df = df.with_column("token_ids", Transcriber()(df["extracted_features"]))
df = df.with_column("transcription", decoder(df["token_ids"]))
df = df.with_column("transcription_length", df["transcription"].str.length())
df = df.exclude("token_ids", "extracted_features", "resampled")
df.write_parquet(OUTPUT_PATH)

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")
