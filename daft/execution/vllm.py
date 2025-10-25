from __future__ import annotations

from typing import Any


class VLLMExecutor:
    def __init__(self, model: str, engine_args: dict[str, Any], generate_args: dict[str, Any]):
        from vllm import LLM

        self.llm = LLM(model=model, **engine_args)
        self.generate_args = generate_args

    def __call__(self, batch: list[str]) -> list[str]:
        output = self.llm.generate(batch, **self.generate_args)
        return [output.outputs[0].text for output in output]
