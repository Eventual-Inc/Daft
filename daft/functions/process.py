"""Process Functions."""

from __future__ import annotations

import subprocess
from typing import Any, Literal

import daft
from daft.datatype import DataType, DataTypeLike
from daft.expressions import Expression


def _convert_stdout(stdout: str | None, return_dtype: DataTypeLike) -> Any:
    """Convert stdout string to Python value matching return_dtype when possible."""
    if stdout is None:
        return None

    inferred_dtype = DataType._infer(return_dtype)
    if inferred_dtype == DataType.string():
        return stdout
    if inferred_dtype == DataType.int64() or inferred_dtype == DataType.int32():
        return int(stdout)
    if inferred_dtype == DataType.float64() or inferred_dtype == DataType.float32():
        return float(stdout)
    if inferred_dtype == DataType.bool():
        normalized_stdout = stdout.strip().lower()
        if normalized_stdout in ("true", "1", "t", "yes"):
            return True
        if normalized_stdout in ("false", "0", "f", "no"):
            return False
        return None

    return stdout


def run_process(
    args: Expression | list[Expression | Any],
    *,
    shell: bool = False,
    on_error: Literal["raise", "ignore", "log"] = "log",
    return_dtype: DataTypeLike = DataType.string(),
) -> Expression:
    """Returns an expression that runs an external process (optionally via a shell) and exposes its stdout as a column.

    This helper wraps a Python UDF around ``subprocess.run`` so it can be used inside DataFrame expressions.

    Args:
        args (Expression | list[Expression | Any]):
            The command to execute. If ``shell=False`` (default), pass a list of arguments, for example ``["ls", "-a", col("path")]``.
            If ``shell=True``, pass a single string expression, for example ``format("echo {}", col("x"))``.
        shell (bool, default=False):
            Whether to execute the command via the system shell (equivalent to ``subprocess.run(..., shell=True)``).
            Using the shell enables pipes and redirection but is more vulnerable to injection. Defaults to ``False``.
        on_error (str, default="log"):
            Whether to log an error when encountering an error, or log a warning and return a null
        return_dtype: Desired Daft data type for the result column. Defaults to a UTF-8 string column.

    Returns:
        Expression: An expression representing the stdout of the process converted to ``return_dtype`` (defaults to a UTF-8 string column).

    Examples:
        >>> import daft
        >>> from daft import col
        >>> from daft.functions import run_process
        >>> df = daft.from_pydict({"a": ["hello"], "b": ["world"]})
        >>> expr = run_process(["echo", col("a"), col("b")])
        >>> df = df.select(expr.alias("out"))
        >>> df.to_pylist()
        [{'out': 'hello world'}]
    """

    def _impl(*argv: Any) -> Any:
        if shell:
            if len(argv) != 1:
                raise ValueError("shell=True expects a single string expression as the command")
            cmd_str = str(argv[0])
            proc = subprocess.run(cmd_str, shell=True, stdout=subprocess.PIPE, text=True, check=True)
        else:
            if len(argv) == 0:
                raise ValueError("shell=False requires at least one argv token")
            tokens = [str(a) for a in argv]
            proc = subprocess.run(tokens, shell=False, stdout=subprocess.PIPE, text=True, check=True)
        stdout = proc.stdout.rstrip("\n") if proc.stdout is not None else None
        return _convert_stdout(stdout, return_dtype)

    decorated = daft.func(return_dtype=return_dtype, on_error=on_error)(_impl)

    if shell:
        expr = Expression._to_expression(args)
        return decorated(expr)
    else:
        if isinstance(args, Expression):
            args = [args]

        if not isinstance(args, list):
            raise TypeError("shell=False args must be a list [cmd, arg1, arg2, ...]")
        expr_args = [Expression._to_expression(v) for v in args]
        return decorated(*expr_args)
