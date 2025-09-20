# Video Object Detection Benchmark

Detects objects in **1,000 videos** using YOLO11n model. Extracts frames, runs object detection, and crops detected objects across distributed GPU nodes.

**Input Dataset**: Hollywood2 video dataset (S3 binary files)
**Output Format**: Parquet with object detections, bounding boxes, and cropped images
**Cluster**: 8 worker nodes using g6.2xlarge instances
**Benchmark Date**: September 19, 2024
**Framework Versions**: Daft 0.6.1, Ray Data 2.49.0, AWS EMR Spark 7.10.0

## Performance Results

| Engine   | Cluster |
|----------|---------|
| Daft     | 12m 17s |
| Ray Data | 34m 20s |
| Spark    | 2h 18m  |
