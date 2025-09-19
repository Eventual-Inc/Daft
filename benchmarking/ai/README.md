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
| Audio Transcription | 113,800 audio files | 6m 25s | 13m 48s | 42m 2s |
| Document Embedding | 10,000 PDFs | 1m 54s | 16m 10s | 8m 4s |
| Image Classification | 803580 images | 3m 14s | 26m 5s | 34m 6s |
| Video Object Detection | 1,000 videos | 12m 17s | 34m 20s | 2h 18m |