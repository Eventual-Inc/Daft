"""LLM Functions."""

from __future__ import annotations

import warnings
from typing import Any, Literal

from daft import DataType, Expression, Series, udf


def llm_generate(
    text: Expression,
    model: str = "facebook/opt-125m",
    provider: Literal["vllm", "openai"] = "vllm",
    concurrency: int = 1,
    batch_size: int | None = None,
    num_cpus: int | None = None,
    num_gpus: int | None = None,
    **generation_config: dict[str, Any],
) -> Expression:
    """A UDF for running LLM inference over an input column of strings.

    This UDF provides a flexible interface for text generation using various LLM providers.
    By default, it uses vLLM for efficient local inference.

    Args:
        text (String Expression):
            The input text column to generate from
        model (str, default="facebook/opt-125m"):
            The model identifier to use for generation
        provider (str, default="vllm"):
            The LLM provider to use for generation. Supported values: "vllm", "openai"
        concurrency (int, default=1):
            The number of concurrent instances of the model to run
        batch_size (int, default=None):
            The batch size for the UDF. If None, the batch size will be determined by defaults based on the provider.
        num_cpus (int, default=None):
            The number of CPUs to use for the UDF
        num_gpus (int, default=None):
            The number of GPUs to use for the UDF
        generation_config (dict, default={}):
            Configuration parameters for text generation (e.g., temperature, max_tokens)

    Returns:
        Expression (String Expression): The generated text column

    Examples:
        Use vLLM provider:
        >>> import daft
        >>> from daft import col
        >>> from daft.functions import llm_generate, format
        >>>
        >>> df = daft.from_pydict({"city": ["Paris", "Tokyo", "New York"]})
        >>> df = df.with_column(
        ...     "description",
        ...     llm_generate(
        ...         format(
        ...             "Describe the main attractions and unique features of this city: {}.",
        ...             col("city"),
        ...         ),
        ...         model="facebook/opt-125m",
        ...     ),
        ... )
        >>> df.collect()

        Use OpenAI provider:
        >>> df = daft.from_pydict({"city": ["Paris", "Tokyo", "New York"]})
        >>> df = df.with_column(
        ...     "description",
        ...     llm_generate(
        ...         format(
        ...             "Describe the main attractions and unique features of this city: {}.",
        ...             col("city"),
        ...         ),
        ...         model="gpt-4o",
        ...         api_key="xxx",
        ...         provider="openai",
        ...     ),
        ... )
        >>> df.collect()

    Note:
        Make sure the required provider packages are installed (e.g. vllm, transformers, openai).
    """
    warnings.warn(
        "This method is deprecated and will be removed in v0.8.0. Use daft.functions.prompt instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    cls: Any = None
    if provider == "vllm":
        cls = _vLLMGenerator
        if batch_size is None:
            batch_size = 1024
    elif provider == "openai":
        cls = _OpenAIGenerator
        if batch_size is None:
            batch_size = 128
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

    return llm_generator(text)


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
        import asyncio

        try:
            from openai import AsyncOpenAI
        except ImportError:
            raise ImportError("Please install the openai package to use this provider.")
        self.model = model
        client_params_keys = ["base_url", "api_key", "timeout", "max_retries"]
        client_params_opts = {key: value for key, value in generation_config.items() if key in client_params_keys}

        self.generation_config = {k: v for k, v in generation_config.items() if k not in client_params_keys}

        self.llm = AsyncOpenAI(**client_params_opts)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()

    def __call__(self, input_prompt_column: Series) -> list[str]:
        import asyncio

        async def get_completion(prompt: str) -> str:
            messages = [{"role": "user", "content": prompt}]
            completion = await self.llm.chat.completions.create(
                model=self.model,
                messages=messages,
                **self.generation_config,
            )
            return completion.choices[0].message.content

        prompts = input_prompt_column.to_pylist()

        async def gather_completions() -> list[str]:
            tasks = [get_completion(prompt) for prompt in prompts]
            return await asyncio.gather(*tasks)

        try:
            outputs = self.loop.run_until_complete(gather_completions())
        except Exception as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.exception(e)
            raise
        return outputs
