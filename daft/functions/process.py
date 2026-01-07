"""Process Functions."""

from __future__ import annotations

import subprocess
from typing import Any, Literal

import daft
from daft.datatype import DataType, DataTypeLike
from daft.expressions import Expression


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
            The command to execute.
            If ``shell=False`` (default), pass a list of arguments, for example ``["ls", "-a", col("path")]``.
            If ``shell=True``, pass a single string expression or a list that will be joined, for example ``"echo hello"`` or ``["echo", "hello"]``.
        shell (bool, default=False):
            Whether to execute the command via the system shell (equivalent to ``subprocess.run(..., shell=True)``).
            Using the shell enables pipes and redirection but is more vulnerable to injection. Defaults to ``False``.
        on_error (Literal["raise", "ignore", "log"], default="log"):
            Whether to log an error when encountering an error, or log a warning and return a null
        return_dtype: Desired Daft data type for the result column. Defaults to a UTF-8 string column.

    Returns:
        Expression: An expression representing the stdout of the process converted to ``return_dtype`` (defaults to a UTF-8 string column).

    Examples:
        >>> import daft
        >>> from daft import col
        >>> from daft.functions import run_process
        >>> df = daft.from_pydict({"a": ["hello"], "b": ["world"]})
        >>> # Without shell
        >>> expr = run_process(["echo", col("a"), col("b")])
        >>> df = df.select(expr.alias("out"))
        >>> df.to_pylist()
        [{'out': 'hello world'}]
        >>>
        >>> # With shell and return_dtype=int
        >>> df = daft.from_pydict({"x": ["hello world"]})
        >>> expr = run_process(("echo " + col("x") + " | wc -c"), shell=True, return_dtype=int)
        >>> df = df.select(expr.alias("word_count"))
        >>> df.to_pylist()
        [{'word_count': 12}]
    """

    @daft.func(return_dtype=return_dtype, on_error=on_error)
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
        return proc.stdout.rstrip("\n") if proc.stdout is not None else None

    if isinstance(args, Expression):
        args = [args]
    elif not isinstance(args, list):
        args = [args]

    expr_args = [Expression._to_expression(v) for v in args]
    return _impl(*expr_args).cast(return_dtype)
