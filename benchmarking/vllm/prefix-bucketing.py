from config import (
    MODEL_NAME,
    SAMPLING_PARAMS,
    INPUT_PATH,
    CONCURRENCY,
    print_benchmark_results,
)
import daft
from daft.functions import prompt
import time


def main():
    print(f"Starting benchmark...")

    daft.set_runner_ray()

    df = daft.read_parquet(INPUT_PATH).into_partitions(8)
    df = df.with_column(
        "output",
        prompt(
            df["prompt"],
            provider="vllm-prefix-caching",
            model=MODEL_NAME,
            engine_args={
                "max_model_len": 4096,
            },
            generate_args={
                "sampling_params": SAMPLING_PARAMS,
            },
            concurrency=CONCURRENCY,
        ),
    )

    print("Running benchmark...")
    start_time = time.perf_counter()
    df = df.collect()
    end_time = time.perf_counter()
    print("Benchmark completed!")

    df = df.with_columns(
        {
            "prompt_len": df["prompt"].length(),
            "output_len": df["output"].length(),
        }
    )
    df.show()

    print_benchmark_results("prefix-bucketing.py", start_time, end_time)


if __name__ == "__main__":
    main()
