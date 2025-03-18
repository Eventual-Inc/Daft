from typing import Literal

from daft import Expression, Series, udf


def llm_generate(
    input_column: Expression,
    model: str = "facebook/opt-125m",
    provider: Literal["vllm"] = "vllm",
    concurrency: int = 1,
    batch_size: int = 1024,
    num_cpus: float | None = None,
    num_gpus: float | None = None,
    **generation_config,
):
    """A UDF for running LLM inference over an input column of strings.

    This UDF provides a flexible interface for text generation using various LLM providers.
    By default, it uses vLLM for efficient local inference.

    Arguments:
    ----------
    model: str, default="facebook/opt-125m"
        The model identifier to use for generation
    provider: str, default="vllm"
        The LLM provider to use for generation. Supported values: "vllm"
    concurrency: int, default=1
        The number of concurrent instances of the model to run
    batch_size: int, default=1024
        The batch size for the UDF
    num_cpus: float, default=None
        The number of CPUs to use for the UDF
    num_gpus: float, default=None
        The number of GPUs to use for the UDF
    **generation_config: dict, default={}
        Configuration parameters for text generation (e.g., temperature, max_tokens)

    Example:
    >>> import daft
    >>> from daft import col
    >>> from daft.functions import llm_generate
    >>> df = daft.read_csv("prompts.csv")
    >>> df = df.with_column("response", llm_generate(col("prompt"), model="facebook/opt-125m"))
    >>> df.collect()

    Notes:
    -----
    Make sure the required provider packages are installed (e.g. vllm, transformers).
    """

    @udf(return_dtype=str, concurrency=concurrency, batch_size=batch_size, num_cpus=num_cpus, num_gpus=num_gpus)
    class LLMGenerator:
        def __init__(
            self,
            model: str,
            provider: str,
            generation_config: dict = {},
        ):
            self.model = model
            self.provider = provider
            self.generation_config = generation_config
            self._initialize_provider()

        def _initialize_vllm(self):
            try:
                from vllm import LLM, SamplingParams
            except ImportError:
                raise ImportError("Please install the vllm package to use this provider.")
            self.llm = LLM(model=self.model)
            self.sampling_params = SamplingParams(**self.generation_config)

        def _initialize_provider(self):
            if self.provider == "vllm":
                self._initialize_vllm()
            else:
                raise ValueError(f"Unsupported provider: {self.provider}")

        def _generate_vllm(self, prompts, **kwargs):
            from vllm import SamplingParams

            outputs = self.llm.generate(prompts, SamplingParams(**self.generation_config, **kwargs))
            return [output.outputs[0].text for output in outputs]

        def __call__(self, input_prompt_column: Series):
            prompts = input_prompt_column.to_pylist()
            if self.provider == "vllm":
                return self._generate_vllm(prompts)
            else:
                raise ValueError(f"Unsupported provider: {self.provider}")

    return LLMGenerator.with_init_args(model=model, provider=provider, generation_config=generation_config)(
        input_column
    )
