from config import (
    MODEL_NAME,
    SAMPLING_PARAMS,
    INPUT_PATH,
    CONCURRENCY,
    print_benchmark_results,
)
from vllm import LLM
import daft
from daft import Series
import time


@daft.cls(max_concurrency=CONCURRENCY, gpus=1)
class VLLM:
    def __init__(self):
        print("Initializing LLM...")
        start_time = time.perf_counter()
        self.llm = LLM(
            model=MODEL_NAME,
            max_model_len=4096,
            disable_log_stats=False,
        )
        end_time = time.perf_counter()
        print(f"LLM initialized in {end_time - start_time:.2f} seconds.")

    @daft.method.batch(return_dtype=str, batch_size=512)
    def generate(self, prompts: Series) -> Series:
        outputs = self.llm.generate(prompts.to_pylist(), SAMPLING_PARAMS)
        print("========== BATCH INFERENCE STATS ==========")
        self.llm.llm_engine.do_log_stats()
        print("===========================================")
        return Series.from_pylist([o.outputs[0].text for o in outputs])


def main():
    print(f"Starting benchmark...")

    daft.set_runner_ray()

    df = daft.read_parquet(INPUT_PATH).into_partitions(32)

    vllm = VLLM()
    df = df.with_column("output", vllm.generate(df["prompt"]))

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

    print_benchmark_results("naive-batch.py", start_time, end_time)


if __name__ == "__main__":
    main()
