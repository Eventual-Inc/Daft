# Audio Transcription Benchmark

Transcribes **113,800 audio files** using Whisper-tiny model. Processes audio resampling, feature extraction, and speech-to-text inference.

**Input Dataset**: Common Voice 17 (S3 parquet format)  
**Output Format**: Parquet with transcriptions and metadata  
**Cluster**: 8 worker nodes using g6.xlarge instances  

## Performance Results

| Engine   | Cluster |
|----------|---------|
| Daft     | 6m 25s  |
| Ray Data | 13m 48s |
| Spark    | 42m 2s |
