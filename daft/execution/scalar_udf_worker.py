from __future__ import annotations

import base64
import pickle
import sys
from typing import Any, Callable

from daft import Expression
from daft.daft import deserialize_literals, serialize_literal


def call_func(
    fn: Callable[..., Any],
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[Any],
) -> list[Any]:
    """Called from Rust to evaluate a Python scalar UDF. Returns a list of Python objects."""
    args, kwargs = original_args

    new_args = [evaluated_args.pop(0) if isinstance(arg, Expression) else arg for arg in args]
    new_kwargs = {key: (evaluated_args.pop(0) if isinstance(arg, Expression) else arg) for key, arg in kwargs.items()}

    output = fn(*new_args, **new_kwargs)
    return output


def main():
    pickled_function = base64.b64decode(sys.argv[1])
    pickled_args = base64.b64decode(sys.argv[2])
    udf_func = pickle.loads(pickled_function)
    bound_args = pickle.loads(pickled_args)

    for line in sys.stdin:
        line = line.strip()
        if line == "EXIT":
            break
        try:
            args = base64.b64decode(line)
            args = deserialize_literals(args)
            result = call_func(udf_func, bound_args, args)
            result = serialize_literal(result)
            result = base64.b64encode(result).decode("utf-8")
            print(result, flush=True)

        except Exception as e:
            res = None
            res = serialize_literal(res)
            res = base64.b64encode(res).decode("utf-8")
            print('stderr', e, file=sys.stderr)
            print('seult', result, file=sys.stderr)
            print(res, flush=True)


if __name__ == "__main__":
    main()
