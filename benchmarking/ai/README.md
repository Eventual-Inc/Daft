# AI Benchmarks

This repository contains performance benchmarks comparing different data processing engines (Daft, Ray Data, and Spark) across various AI and multimodal data processing workloads.

## Overview

The benchmarks cover four different workload types, each designed to test different aspects of multimodal data processing:

1. **Audio Transcription** - Transcribing audio files
2. **Document Embedding** - Generating embeddings for PDF documents
3. **Image Classification** - Classify images
4. **Video Object Detection** - Detect objects in videos

## Performance Results Summary

| Workload | Data Size | Daft | Ray Data | Spark |
|----------|-----------|------|----------|-------|
| Audio Transcription | 113,800 audio files | 6m 22s | 29m 20s | 25m 46s |
| Document Embedding | 10,000 PDFs | 1m 54s | 14m 32s | 8m 4s |
| Image Classification | 803,580 images | 4m 23s | 23m 30s | 45m 7s |
| Video Object Detection | 1,000 videos | 11m 46s | 25m 54s | 3h 36m |
