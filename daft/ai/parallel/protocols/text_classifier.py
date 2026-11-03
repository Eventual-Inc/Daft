from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from daft.ai.protocols import TextClassifier, TextClassifierDescriptor
from daft.ai.typing import ClassifyTextOptions, Label, Options

if TYPE_CHECKING:
    import requests


@dataclass
class ParallelTextClassifierDescriptor(TextClassifierDescriptor):
    provider_name: str
    api_key: str
    base_url: str
    timeout: float
    mode: str
    classify_options: ClassifyTextOptions

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.mode

    def get_options(self) -> Options:
        return dict(self.classify_options)

    def instantiate(self) -> TextClassifier:
        return ParallelTextClassifier(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=self.timeout,
            mode=self.mode,
        )


class ParallelTextClassifier(TextClassifier):
    def __init__(self, api_key: str, base_url: str, timeout: float, mode: str):
        import requests

        self._url = f"{base_url}/v1/decisions"
        self._timeout = timeout
        self._mode = mode
        self._client: requests.Session = requests.Session()
        self._client.headers.update({"x-api-key": api_key, "Content-Type": "application/json"})

    def classify_text(self, text: list[str], labels: Label | list[Label]) -> list[Label]:
        label_list = list(dict.fromkeys([labels] if isinstance(labels, str) else labels))
        if not 2 <= len(label_list) <= 255:
            raise ValueError(f"Parallel Decisions requires between 2 and 255 unique labels, got {len(label_list)}")

        present = [(index, value) for index, value in enumerate(text) if value is not None]
        if not present:
            return cast("list[Label]", [None] * len(text))

        texts = [value for _, value in present]
        criteria = {label: f"Use when {label!r} best describes the text." for label in label_list}
        questions = {
            f"row_{index}": {
                "type": "choice",
                "instructions": f"Which label best classifies texts[{offset}]?",
                "criteria": criteria,
            }
            for offset, (index, _) in enumerate(present)
        }
        response = self._client.post(
            self._url,
            json={"mode": self._mode, "state": {"texts": texts}, "questions": questions},
            timeout=self._timeout,
        )
        response.raise_for_status()
        payload: Any = response.json()
        answers = payload.get("answers") if isinstance(payload, dict) else None
        if not isinstance(answers, dict):
            raise RuntimeError("Parallel Decisions response has no answers object")

        results: list[Label | None] = [None] * len(text)
        for index, _ in present:
            name = f"row_{index}"
            answer = answers.get(name)
            if not isinstance(answer, dict):
                raise RuntimeError(f"Parallel Decisions response has no answer for {name}")
            if answer.get("type") == "refusal":
                raise RuntimeError(f"Parallel Decisions refused to classify {name}")
            choice = answer.get("choice")
            if answer.get("type") != "choice" or choice not in label_list:
                raise RuntimeError(f"Parallel Decisions returned an invalid choice for {name}: {choice!r}")
            results[index] = choice
        return cast("list[Label]", results)
