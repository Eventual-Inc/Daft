from vllm import SamplingParams


MODEL_NAME = "Qwen/Qwen3-8B"
INPUT_PATH = (
    "s3://my-bucket/vllm-prefix-caching-partitioned/200k_0-5_512.parquet"
)
OUTPUT_LEN = 128
SAMPLING_PARAMS = SamplingParams(min_tokens=OUTPUT_LEN, max_tokens=OUTPUT_LEN)
CONCURRENCY = 128


def print_benchmark_results(
    script: str, start_time: float, end_time: float
):
    print(f"========== BENCHMARK RESULTS ==========")
    print(f"Script: {script}")
    print(f"Execution time: {end_time - start_time:.2f} seconds.")
    print()
    print("Benchmark configuration:")
    print(f"\t{MODEL_NAME=}")
    print(f"\t{INPUT_PATH=}")
    print(f"\t{OUTPUT_LEN=}")
    print(f"\t{CONCURRENCY=}")
    print(f"======================================")
