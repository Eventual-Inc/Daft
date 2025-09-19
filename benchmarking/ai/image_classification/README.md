# Image Classification Benchmark

Classifies **803,580 images** using ResNet18 model. Downloads images, applies preprocessing transforms, and runs inference to predict ImageNet labels across distributed GPU nodes.

**Input Dataset**: ImageNet benchmark dataset (S3 parquet format)  
**Output Format**: Parquet with image URLs and predicted labels  
**Cluster**: 8 worker nodes using g6.2xlarge instances  

## Performance Results

| Engine   | Cluster |
|----------|---------|
| Daft     | 3m 14s  |
| Ray Data | 26m 5s  |
| Spark    | 34m 6s  |