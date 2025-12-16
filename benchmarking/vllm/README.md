# vLLM Benchmarks

This directory contains benchmarks for running batch inference with vLLM.

## Setup

1. Install the dependencies:
  - `vllm`
  - `daft`
  - `ray`
2. Run the `generate_data.ipynb` notebook to generate the datasets.
3. Update `config.py` with the correct dataset path and other parameters.
4. Run the benchmark scripts on a Ray cluster.

## Benchmarks
- `naive-batch.py`: Simple batch inference using a Daft batch function and vLLM's `LLM` class.
- `naive-batch-sorted.py`: Same as `naive-batch.py`, but with the prompts sorted.
- `continuous-batch.py`: Continuous batching using the `vllm-prefix-caching` provider, with prefix routing disabled.
- `continuous-batch-sorted.py`: Same as `continuous-batch.py`, but with the prompts sorted.
- `prefix-bucketing.py`: Both continuous batching and prefix bucketing using the `vllm-prefix-caching` provider.
- `ray-data.py`: Ray Data batching using `ray.data.llm.build_llm_processor`.
