# Video Object Detection Benchmark

Detects objects in **1,000 videos** using YOLO11n model. Extracts frames, runs object detection, and crops detected objects across distributed GPU nodes.

**Input Dataset**: Hollywood2 video dataset (S3 binary files)
**Output Format**: Parquet with object detections, bounding boxes, and cropped images
**Cluster**: 8 worker nodes using g6.xlarge instances
**Benchmark Date**: September 22, 2024
**Framework Versions**: Daft 0.6.2, Ray Data 2.49.2, AWS EMR Spark 7.10.0

## Performance Results

| Engine   | Runtime |
|----------|---------|
| Daft     | 11m 46s |
| Ray Data | 25m 54s |
| Spark    | 3h  36m |
