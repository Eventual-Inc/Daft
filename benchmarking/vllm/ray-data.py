import ray
from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor
import time

from config import (
    MODEL_NAME,
    SAMPLING_PARAMS,
    INPUT_PATH,
    CONCURRENCY,
    print_benchmark_results,
)

def main():
    print(f"Starting benchmark...")

    ray.data.DataContext.get_current().enable_rich_progress_bars = True

    config = vLLMEngineProcessorConfig(
        model_source=MODEL_NAME,
        engine_kwargs=dict(
            enable_prefix_caching=True,
            max_model_len=4096,
        ),
        batch_size=16,
        concurrency=CONCURRENCY,
    )

    processor = build_llm_processor(config)

    print("Running benchmark...")
    start_time = time.perf_counter()

    ds = ray.data.read_parquet(INPUT_PATH)
    ds = ds.map(lambda row: {"messages": [{"role": "user", "content": row["prompt"]}], "sampling_params": SAMPLING_PARAMS})
    output_ds = processor(ds)
    output_ds.take_all()
    print(output_ds)

    end_time = time.perf_counter()

    print_benchmark_results("ray-data.py", start_time, end_time)

if __name__ == "__main__":
    main()
