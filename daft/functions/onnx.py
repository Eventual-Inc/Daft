"""ONNX model inference as an expression."""

from __future__ import annotations

from daft.daft import PyExpr
from daft.datatype import DataType
from daft.expressions import Expression
from daft.io import IOConfig


def onnx_model(
    model: str,
    *inputs: Expression,
    return_dtype: DataType = DataType.tensor(DataType.float32()),
    io_config: IOConfig | None = None,
) -> Expression:
    """Run an ONNX model as a Daft expression.

    The ONNX model's graph inputs are bound positionally to the provided
    input expressions. The returned expression produces the model's outputs.

    Args:
        model: Path or URI to a ``.onnx`` model file (local, S3, GCS, etc.).
        *inputs: One expression per ONNX graph input, in order.
        return_dtype: The return data type of the model output.
        io_config: IO configuration for loading the model from remote storage.

    Returns:
        An expression representing the ONNX model's output(s).

    Examples:
        >>> import daft
        >>> from daft import col, DataType
        >>> from daft.functions import onnx_model
        >>>
        >>> df = daft.from_pydict({"x": [[1.0, -2.0, 3.0]]})
        >>> df = df.select("*", onnx_model("model.onnx", col("x")))
        >>>
        >>> # Load from S3
        >>> df = df.select("*", onnx_model("s3://bucket/model.onnx", col("x")))
    """
    input_pyexprs = [e._expr for e in inputs]
    io_config_native = io_config._io_config if io_config is not None else None
    return Expression._from_pyexpr(
        PyExpr.onnx_model(model, input_pyexprs, return_dtype._dtype, io_config_native)
    )
