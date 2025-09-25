# Audio Transcription Benchmark

Transcribes **113,800 audio files** using Whisper-tiny model. Processes audio resampling, feature extraction, and speech-to-text inference.

**Input Dataset**: Common Voice 17 (S3 parquet format)
**Output Format**: Parquet with transcriptions and metadata
**Cluster**: 8 worker nodes using g6.xlarge instances
**Benchmark Date**: September 22, 2024
**Framework Versions**: Daft 0.6.2, Ray Data 2.49.2, AWS EMR Spark 7.10.0

## Performance Results

| Engine   | Runtime |
|----------|---------|
| Daft     | 6m 22s  |
| Ray Data | 29m 20s |
| Spark    | 25m 46s  |
