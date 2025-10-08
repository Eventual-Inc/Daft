# Image Classification Benchmark

Classifies **803,580 images** using ResNet18 model. Downloads images, applies preprocessing transforms, and runs inference to predict ImageNet labels across distributed GPU nodes.

**Input Dataset**: ImageNet benchmark dataset (S3 parquet format)
**Output Format**: Parquet with image URLs and predicted labels
**Cluster**: 8 worker nodes using g6.xlarge instances
**Benchmark Date**: September 22, 2024
**Framework Versions**: Daft 0.6.2, Ray Data 2.49.2, AWS EMR Spark 7.10.0

## Performance Results

| Engine   | Runtime |
|----------|---------|
| Daft     | 4m 23s  |
| Ray Data | 23m 30s |
| Spark    | 45m 7s  |
