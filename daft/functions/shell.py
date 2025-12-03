from __future__ import annotations

import logging
import os
import shlex
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Literal

import daft
from daft.datatype import DataType
from daft.expressions import Expression

ArgStyle = Literal["separate", "equals"]
OnError = Literal["raise", "ignore", "log"]

logger = logging.getLogger(__name__)


@dataclass
class ShellOpOptions:
    # result configuration
    return_struct: bool = False
    capture_stderr: bool = False
    include_exit_code: bool = False
    include_duration_ms: bool = False

    # subprocess configuration
    timeout_seconds: float | None = None
    cwd: str | None = None
    env: dict[str, str] | None = None
    use_shell: bool = False  # when True, build command string and run with shell=True

    # UDF execution configuration
    use_process: bool | None = None
    max_concurrency: int | None = None
    on_error: OnError | None = None

    # argument formatting
    arg_style: ArgStyle = "separate"


class ShellRunner:
    """Row-wise shell command runner as a Daft class.

    This class is dynamically wrapped with `@daft.cls(...)` in `shell_op` to apply
    per-call execution options like `use_process`, `max_concurrency`, and `on_error`.
    """

    def __init__(self, cmd: str, options: ShellOpOptions, arg_style: ArgStyle):
        self.cmd = cmd
        self.options = options
        self.arg_style = arg_style

    def _build_cmd_tokens(self, *args: Any, **kwargs: Any) -> list[str]:
        tokens = shlex.split(self.cmd)

        for a in args:
            tokens.append(str(a))

        if kwargs:
            for k, v in kwargs.items():
                key = str(k)
                if isinstance(v, bool) or v is None:
                    if v is True:
                        tokens.append(key)
                    continue

                val = str(v)
                if self.arg_style == "equals":
                    tokens.append(f"{key}={val}")
                else:
                    tokens.extend([key, val])

        return tokens

    def _build_cmd_str(self, *args: Any, **kwargs: Any) -> str:
        tokens = self._build_cmd_tokens(*args, **kwargs)
        try:
            return shlex.join(tokens)
        except AttributeError:

            def _q(t: str) -> str:
                return shlex.quote(t)

            return " ".join(_q(t) for t in tokens)

    def _run_subprocess(self, *args: Any, **kwargs: Any) -> tuple[str | None, str | None, int | None, int | None]:
        env = os.environ.copy()
        if self.options.env:
            env.update({str(k): str(v) for k, v in self.options.env.items()})

        t0 = time.perf_counter()
        try:
            if self.options.use_shell:
                cmd_str = self._build_cmd_str(*args, **kwargs)
                proc = subprocess.run(
                    cmd_str,
                    cwd=self.options.cwd,
                    env=env,
                    timeout=self.options.timeout_seconds,
                    capture_output=True,
                    text=True,
                    shell=True,
                )
            else:
                cmd_tokens = self._build_cmd_tokens(*args, **kwargs)
                proc = subprocess.run(
                    cmd_tokens,
                    cwd=self.options.cwd,
                    env=env,
                    timeout=self.options.timeout_seconds,
                    capture_output=True,
                    text=True,
                    shell=False,
                )
            t1 = time.perf_counter()
            duration_ms = int((t1 - t0) * 1000)

            # Determine outputs respecting capture/include options
            stdout: str | None = proc.stdout.rstrip("\n")
            stderr: str | None = proc.stderr.rstrip("\n") if self.options.capture_stderr else None
            exit_code: int | None = proc.returncode if self.options.include_exit_code else None

            if proc.returncode != 0:
                oe = self.options.on_error or "log"
                if oe == "raise":
                    raise RuntimeError(f"shell_op failed with exit_code={proc.returncode}: stderr={proc.stderr!r}")
                if oe == "log":
                    logger.error("shell_op non-zero exit: code=%s stderr=%s", proc.returncode, proc.stderr)
                stdout = None

            return stdout, stderr, exit_code, duration_ms if self.options.include_duration_ms else None
        except subprocess.TimeoutExpired as e:
            t1 = time.perf_counter()
            duration_ms = int((t1 - t0) * 1000)
            oe = self.options.on_error or "raise"
            if oe == "raise":
                raise
            # ignore/log: pack timeout info in stderr if captured
            if oe == "log":
                logger.error("shell_op timeout: %s", e)
            stdout = None
            stderr = f"TimeoutExpired: {e}" if self.options.capture_stderr else None
            if self.options.include_exit_code:
                exit_code = -1
            else:
                exit_code = None
            return stdout, stderr, exit_code, duration_ms if self.options.include_duration_ms else None

    @daft.method(return_dtype=DataType.string())
    def run_str(self, *args: Any, **kwargs: Any) -> str | None:
        stdout, _stderr, _exit_code, _duration_ms = self._run_subprocess(*args, **kwargs)
        # on error ignore/log or timeout: return None
        return stdout

    @daft.method(
        return_dtype=DataType.struct(
            {
                "stdout": DataType.string(),
                "stderr": DataType.string(),
                "exit_code": DataType.int32(),
                "duration_ms": DataType.int64(),
            }
        ),
        unnest=True,
    )
    def run_struct(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        stdout, stderr, exit_code, duration_ms = self._run_subprocess(*args, **kwargs)
        # Fill optional fields with None when not requested
        result: dict[str, Any] = {
            "stdout": stdout,
            "stderr": stderr if self.options.capture_stderr else None,
            "exit_code": exit_code if self.options.include_exit_code else None,
            "duration_ms": duration_ms if self.options.include_duration_ms else None,
        }
        return result


def _to_expr(v: Any) -> Expression:
    return v if isinstance(v, Expression) else daft.lit(v)


def shell_op(
    cmd: str,
    params: list[Expression | Any] | dict[str, Expression | Any],
    options: ShellOpOptions | None = None,
) -> Expression:
    """Execute a shell command per row and return a Daft Expression.

    Args:
        cmd (str):
            Base command string, e.g., "python script.py".
        params (list[Expression | Any] or dict[str, Expression | Any] or Expression):
            Per-row arguments passed to the command. Lists are appended as positional
            arguments. Dict keys are flags (for example "--start"), values are the
            corresponding argument values. A single Expression is treated as one
            positional argument.
        options (ShellOpOptions, optional):
            Controls return format, subprocess behavior and UDF execution. The following
            fields are supported:

            return_struct : bool, default False
                When False, returns a string column containing stdout. When True,
                returns a struct column; see Returns for field names.
            capture_stderr : bool, default False
                Capture stderr into the struct field "stderr" when ``return_struct=True``.
            include_exit_code : bool, default False
                Include process exit code in the struct field "exit_code" when
                ``return_struct=True``. On a timeout, the exit code is ``-1``.
            include_duration_ms : bool, default False
                Include execution duration in milliseconds in the struct field
                "duration_ms" when ``return_struct=True``.
            timeout_seconds : float, optional
                Subprocess timeout in seconds. On timeout, behavior is controlled by
                ``on_error``; see Notes.
            cwd : str, optional
                Working directory for subprocess execution.
            env : dict[str, str], optional
                Extra environment variables for the subprocess (merged with the worker
                environment).
            use_shell : bool, default False
                When True, execute through the system shell (for example ``/bin/sh`` or
                ``bash``). This enables pipes and redirection but increases security risk.
                When False, execute the command as a token list.
            use_process : bool, optional
                UDF execution mode: True for a separate process, False for a thread,
                None to use Daft defaults.
            max_concurrency : int, optional
                Max number of concurrent executions per worker for this UDF call.
            on_error : {"raise", "ignore", "log"}, optional, default "log"
                Error handling for non-zero exit codes or timeouts. "raise" raises,
                "ignore" returns ``None`` for stdout while keeping structured fields if
                enabled, "log" behaves like "ignore" and logs a message on the worker.
            arg_style : {"separate", "equals"}, default "separate"
                Formatting for named arguments. "separate" yields ``--key value``;
                "equals" yields ``--key=value``.

    Returns:
        Expression
            Returns a String expression containing stdout when ``options.return_struct`` is
            False. When ``options.return_struct`` is True, returns a Struct expression with
            fields: ``stdout`` (string), ``stderr`` (string), ``exit_code`` (int32), ``duration_ms``
            (int64). Disabled fields are returned as ``None``.

    Note:
        - Security: Prefer ``use_shell=False``. Only enable ``use_shell=True`` when shell
          features (pipes, redirection) are required and the command is trusted.
        - Boolean flags: For a named key passed with a boolean value, True appends the
          key without a value; False or None omit the key.
        - Timeout semantics: When ``timeout_seconds`` is reached, ``on_error`` controls the
          outcome. With ``include_exit_code=True`` the exit code is ``-1``; ``stderr`` contains
          a "TimeoutExpired" description when ``capture_stderr=True``.

    Examples:
        Use positional arguments:

        >>> df = daft.from_pydict({"a": [1, 2], "b": [2, 4]})
        >>> expr = daft.functions.shell_op(
        ...     cmd="echo",
        ...     params=[df["a"], df["b"]],
        ... )
        >>> df = df.select(expr.alias("out"))

        Structured return with stderr and exit code:

        >>> df = daft.from_pydict({"path": ["/a", "/b"]})
        >>> opts = daft.functions.shell.ShellOpOptions(
        ...     return_struct=True, capture_stderr=True, include_exit_code=True, include_duration_ms=True
        ... )
        >>> expr = daft.functions.shell_op(
        ...     cmd="ls",
        ...     params=["-la", df["path"]],
        ...     options=opts,
        ... )
        >>> df = df.select(expr)  # columns: stdout, stderr, exit_code, duration_ms

    """
    options = options or ShellOpOptions()
    arg_style: ArgStyle = options.arg_style or "separate"

    # Dynamically wrap the class to apply UDF execution options per call
    WrappedRunner = daft.cls(
        ShellRunner,
        use_process=options.use_process,
        max_concurrency=options.max_concurrency,
        on_error=options.on_error,
    )
    runner = WrappedRunner(cmd, options, arg_style)

    if isinstance(params, list):
        expr_params = [_to_expr(v) for v in params]
        if options.return_struct:
            return runner.run_struct(*expr_params)
        else:
            return runner.run_str(*expr_params)
    elif isinstance(params, dict):
        expr_kwargs = {k: _to_expr(v) for k, v in params.items()}
        if options.return_struct:
            return runner.run_struct(**expr_kwargs)
        else:
            return runner.run_str(**expr_kwargs)
    elif isinstance(params, Expression):
        # Support a single Expression object as parameter
        expr_param = _to_expr(params)
        if options.return_struct:
            return runner.run_struct(expr_param)
        else:
            return runner.run_str(expr_param)
    else:
        raise TypeError("params must be a list or dict of arguments")
