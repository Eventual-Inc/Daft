# Document Embedding Benchmark

Generates embeddings for **10,000 PDF documents**. Extracts text, chunks content, and creates 384-dimensional embeddings using sentence-transformers/all-MiniLM-L6-v2 model across distributed nodes.

**Input Dataset**: Digital Corpora PDF metadata (S3 parquet format)
**Output Format**: Parquet with embeddings, text chunks, and metadata
**Cluster**: 8 worker nodes using g6.xlarge instances
**Benchmark Date**: September 22, 2024
**Framework Versions**: Daft 0.6.2, Ray Data 2.49.2, AWS EMR Spark 7.10.0

## Performance Results

| Engine   | Runtime |
|----------|---------|
| Daft     | 1m 54s  |
| Ray Data | 14m 32s |
| Spark    | 8m 4s   |
