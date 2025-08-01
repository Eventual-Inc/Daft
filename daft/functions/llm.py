from __future__ import annotations

from typing import Any, Literal

from daft import DataType, Expression, Series, udf


def llm_generate(
    input_column: Expression,
    model: str = "facebook/opt-125m",
    provider: Literal["vllm", "openai"] = "vllm",
    concurrency: int = 1,
    batch_size: int = 1024,
    num_cpus: int | None = None,
    num_gpus: int | None = None,
    **generation_config: dict[str, Any],
) -> Expression:
    """A UDF for running LLM inference over an input column of strings.

    This UDF provides a flexible interface for text generation using various LLM providers.
    By default, it uses vLLM for efficient local inference.

    Args:
        model: str, default="facebook/opt-125m"
            The model identifier to use for generation
        provider: str, default="vllm"
            The LLM provider to use for generation. Supported values: "vllm", "openai"
        concurrency: int, default=1
            The number of concurrent instances of the model to run
        batch_size: int, default=1024
            The batch size for the UDF
        num_cpus: float, default=None
            The number of CPUs to use for the UDF
        num_gpus: float, default=None
            The number of GPUs to use for the UDF
        generation_config: dict, default={}
            Configuration parameters for text generation (e.g., temperature, max_tokens)

    Examples:
        Use vLLM provider:
        >>> import daft
        >>> from daft import col
        >>> from daft.functions import llm_generate
        >>> df = daft.read_csv("prompts.csv")
        >>> df = df.with_column("response", llm_generate(col("prompt"), model="facebook/opt-125m"))
        >>> df.collect()

        Use OpenAI provider:
        >>> df = daft.read_csv("prompts.csv")
        >>> df = df.with_column("response", llm_generate(col("prompt"), model="gpt-4o", api_key="xxx", provider="openai"))
        >>> df.collect()

    Note:
        Make sure the required provider packages are installed (e.g. vllm, transformers, openai).
    """
    cls: Any = None
    if provider == "vllm":
        cls = _vLLMGenerator
    elif provider == "openai":
        cls = _OpenAIGenerator
    else:
        raise ValueError(f"Unsupported provider: {provider}")

    llm_generator = udf(
        return_dtype=DataType.string(),
        batch_size=batch_size,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        concurrency=concurrency,
    )(cls).with_init_args(
        model=model,
        generation_config=generation_config,
    )

    return llm_generator(input_column)


class _vLLMGenerator:
    def __init__(
        self,
        model: str = "facebook/opt-125m",
        generation_config: dict[str, Any] = {},
    ) -> None:
        try:
            from vllm import LLM
        except ImportError:
            raise ImportError("Please install the vllm package to use this provider.")
        self.model = model
        self.generation_config = generation_config
        self.llm = LLM(model=self.model)

    def __call__(self, input_prompt_column: Series) -> list[str]:
        from vllm import SamplingParams

        prompts = input_prompt_column.to_pylist()
        outputs = self.llm.generate(prompts, SamplingParams(**self.generation_config))
        return [output.outputs[0].text for output in outputs]


class _OpenAIGenerator:
    def __init__(
        self,
        model: str = "gpt-4o",
        generation_config: dict[str, Any] = {},
    ) -> None:
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("Please install the openai package to use this provider.")
        self.model = model
        client_params_keys = ["base_url", "api_key", "timeout", "max_retries"]
        client_params_opts = {key: value for key, value in generation_config.items() if key in client_params_keys}

        self.generation_config = {k: v for k, v in generation_config.items() if k not in client_params_keys}

        self.llm = OpenAI(**client_params_opts)

    def __call__(self, input_prompt_column: Series) -> list[str]:
        prompts = input_prompt_column.to_pylist()
        outputs = []
        for prompt in prompts:
            messages = [{"role": "user", "content": prompt}]
            completion = self.llm.chat.completions.create(
                model=self.model,
                messages=messages,
                **self.generation_config,
            )
            outputs.append(completion.choices[0].message.content)
        return outputs
