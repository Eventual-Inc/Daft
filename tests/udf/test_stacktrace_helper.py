"""Helper script for testing stack trace extraction from Rust"""


def simple_error(x: int) -> int:
    """A simple function that raises an error"""
    if x < 0:
        raise ValueError(f"Negative value not allowed: {x}")
    return x * 2


def nested_error(x: int) -> int:
    """A function that calls another function that errors"""
    def helper(y: int) -> int:
        if y == 0:
            raise ZeroDivisionError("Cannot divide by zero")
        return 100 / y
    return helper(x)


def deep_stack_error(n: int) -> int:
    """A function with a deep call stack"""
    if n <= 0:
        raise RuntimeError(f"Invalid depth: {n}")
    if n == 1:
        return 1
    return deep_stack_error(n - 1) + 1


def udf_like_function(value: int, multiplier: int = 2) -> int:
    """Simulates a UDF that processes a row"""
    if value < 0:
        raise ValueError(f"Negative value not allowed: {value}")
    if value > 1000:
        raise OverflowError(f"Value too large: {value}")
    if multiplier == 0:
        raise ZeroDivisionError("Multiplier cannot be zero")
    return value * multiplier


def complex_udf_error(data: dict) -> dict:
    """Simulates a more complex UDF with nested errors"""
    try:
        x = data["x"]
        y = data["y"]
        result = x / y
        return {"result": result}
    except KeyError as e:
        raise ValueError(f"Missing required key: {e}") from e
    except ZeroDivisionError:
        raise ValueError("Division by zero in UDF") from None

