# Image Classification Benchmark

Classifies **803,580 images** using ResNet18 model. Downloads images, applies preprocessing transforms, and runs inference to predict ImageNet labels across distributed GPU nodes.

**Input Dataset**: ImageNet benchmark dataset (S3 parquet format)
**Output Format**: Parquet with image URLs and predicted labels
**Cluster**: 8 worker nodes using g6.2xlarge instances
**Benchmark Date**: September 19, 2024
**Framework Versions**: Daft 0.6.1, Ray Data 2.49.0, AWS EMR Spark 7.10.0

## Performance Results

| Engine   | Runtime |
|----------|---------|
| Daft     | 3m 14s  |
| Ray Data | 26m 5s  |
| Spark    | 34m 6s  |
