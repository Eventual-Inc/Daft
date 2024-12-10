# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

"""The `read` function below is sourced from: https://packaging.python.org/en/latest/specifications/inline-script-metadata/#inline-script-metadata."""

import re

import tomllib

REGEX = r"(?m)^# /// (?P<type>[a-zA-Z0-9-]+)$\s(?P<content>(^#(| .*)$\s)+)^# ///$"


def read(script: str) -> dict | None:
    name = "script"
    matches = list(filter(lambda m: m.group("type") == name, re.finditer(REGEX, script)))
    if len(matches) > 1:
        raise ValueError(f"Multiple {name} blocks found")
    elif len(matches) == 1:
        content = "".join(
            line[2:] if line.startswith("# ") else line[1:]
            for line in matches[0].group("content").splitlines(keepends=True)
        )
        return tomllib.loads(content)
    else:
        return None
