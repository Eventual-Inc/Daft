# functions numeric

## abs

```python
abs(expr: Expression) -> Expression
```

Absolute of a numeric expression.

## arccos

```python
arccos(expr: Expression) -> Expression
```

The elementwise arc cosine of a numeric expression.

## arccosh

```python
arccosh(expr: Expression) -> Expression
```

The elementwise inverse hyperbolic cosine of a numeric expression.

## arcsin

```python
arcsin(expr: Expression) -> Expression
```

The elementwise arc sine of a numeric expression.

## arcsinh

```python
arcsinh(expr: Expression) -> Expression
```

The elementwise inverse hyperbolic sine of a numeric expression.

## arctan

```python
arctan(expr: Expression) -> Expression
```

The elementwise arc tangent of a numeric expression.

## arctan2

```python
arctan2(y: Expression, x: Expression) -> Expression
```

Calculates the four quadrant arctangent of coordinates (y, x), in radians.

* ``x = 0``, ``y = 0``: ``0``
* ``x >= 0``: ``[-pi/2, pi/2]``
* ``y >= 0``: ``(pi/2, pi]``
* ``y < 0``: ``(-pi, -pi/2)``

## arctanh

```python
arctanh(expr: Expression) -> Expression
```

The elementwise inverse hyperbolic tangent of a numeric expression.

## between

```python
between(expr: Expression, lower: Expression | int | float, upper: Expression | int | float) -> Expression
```

Checks if values in the Expression are between lower and upper, inclusive.

Args:
    expr: The expression to check
    lower: Lower bound (inclusive)
    upper: Upper bound (inclusive)

Returns:
    Expression: Boolean Expression indicating whether values are between lower and upper, inclusive.

## bin

```python
bin(expr: Expression) -> Expression
```

Returns the string representation of the binary value of an integer.

Inputs are promoted to 64-bit before conversion; negatives produce
64-character two's-complement strings (e.g. ``bin(-1)`` returns 64 ones).

## cbrt

```python
cbrt(expr: Expression) -> Expression
```

The cube root of a numeric expression.

## ceil

```python
ceil(expr: Expression) -> Expression
```

The ceiling of a numeric expression.

## clip

```python
clip(expr: Expression, min: Expression | None=None, max: Expression | None=None) -> Expression
```

Clips an expression to the given minimum and maximum values.

Args:
    expr: The expression to clip
    min: Minimum value to clip to. If None (or column value is Null), no lower clipping is applied.
    max: Maximum value to clip to. If None (or column value is Null), no upper clipping is applied.

## conv

```python
conv(expr: Expression, from_base: int, to_base: int) -> Expression
```

Converts a number from base ``from_base`` to base ``to_base`` (bases 2-36).

Positive ``to_base`` interprets negative inputs as 64-bit two's complement
(``conv("-1", 10, 16) == "FFFFFFFFFFFFFFFF"``); negative ``to_base`` returns
a signed result (``conv("-1", 10, -16) == "-1"``). Trailing invalid characters
are silently truncated (``conv("11abc", 10, 16) == "B"``). Returns NULL on
out-of-range bases, on u64 overflow during parsing, or when a negated
magnitude exceeds 2^63.

## cos

```python
cos(expr: Expression) -> Expression
```

The elementwise cosine of a numeric expression.

## cosh

```python
cosh(expr: Expression) -> Expression
```

The elementwise hyperbolic cosine of a numeric expression.

## cot

```python
cot(expr: Expression) -> Expression
```

The elementwise cotangent of a numeric expression.

## csc

```python
csc(expr: Expression) -> Expression
```

The elementwise cosecant of a numeric expression.

## degrees

```python
degrees(expr: Expression) -> Expression
```

The elementwise degrees of a numeric expression.

## e

```python
e() -> Expression
```

Returns Euler's number (e = 2.71828...).

## exp

```python
exp(expr: Expression) -> Expression
```

The e^expr of a numeric expression.

## expm1

```python
expm1(expr: Expression) -> Expression
```

The e^expr - 1 of a numeric expression.

## factorial

```python
factorial(expr: Expression) -> Expression
```

Returns the factorial of a non-negative integer.

## fill_nan

```python
fill_nan(expr: Expression, fill_value: Expression) -> Expression
```

Fills NaN values in the Expression with the provided fill_value.

Returns:
    Expression: Expression with Nan values filled with the provided fill_value

## floor

```python
floor(expr: Expression) -> Expression
```

The floor of a numeric expression.

## hypot

```python
hypot(a: Expression, b: Expression) -> Expression
```

Returns sqrt(a^2 + b^2), the Euclidean norm.

## is_inf

```python
is_inf(expr: Expression) -> Expression
```

Checks if values in the Expression are Infinity.

Returns:
    Expression: Boolean Expression indicating whether values are Infinity.

Note:
    Nulls will be propagated! I.e. this operation will return a null for null values.

## is_nan

```python
is_nan(expr: Expression) -> Expression
```

Checks if values are NaN (a special float value indicating not-a-number).

Returns:
    Expression: Boolean Expression indicating whether values are invalid.

Note:
    Nulls will be propagated! I.e. this operation will return a null for null values.

## ln

```python
ln(expr: Expression) -> Expression
```

The elementwise natural log of a numeric expression.

## log

```python
log(expr: Expression, base: int | float=math.e) -> Expression
```

The elementwise log with given base, of a numeric expression.

Args:
    expr: The expression to take the logarithm of
    base: The base of the logarithm. Defaults to e.

## log10

```python
log10(expr: Expression) -> Expression
```

The elementwise log base 10 of a numeric expression.

## log1p

```python
log1p(expr: Expression) -> Expression
```

The ln(expr + 1) of a numeric expression.

## log2

```python
log2(expr: Expression) -> Expression
```

The elementwise log base 2 of a numeric expression.

## negate

```python
negate(expr: Expression) -> Expression
```

The negative of a numeric expression.

## not_nan

```python
not_nan(expr: Expression) -> Expression
```

Checks if values are not NaN (a special float value indicating not-a-number).

Returns:
    Expression: Boolean Expression indicating whether values are not invalid.

Note:
    Nulls will be propagated! I.e. this operation will return a null for null values.

## pi

```python
pi() -> Expression
```

Returns the mathematical constant pi (3.14159...).

## pmod

```python
pmod(a: Expression, b: Expression) -> Expression
```

Returns the positive modulo of ``a`` by ``b``.

Computes ``r = a % b``; returns ``r`` when ``r >= 0`` and ``(r + b) % b`` otherwise.
Examples: ``pmod(-7, 3) == 2``, ``pmod(7, -3) == 1``, ``pmod(-7, -3) == -1``.
Returns NULL when ``b`` is 0.

## pow

```python
pow(base: Expression, expr: Expression) -> Expression
```

The base^expr of a numeric expression.

## power

```python
power(base: Expression, expr: Expression) -> Expression
```

The base^expr of a numeric expression.

## radians

```python
radians(expr: Expression) -> Expression
```

The elementwise radians of a numeric expression.

## round

```python
round(expr: Expression, decimals: Expression | int=0) -> Expression
```

The round of a numeric expression.

Args:
    expr: The expression to round
    decimals: number of decimal places to round to. Defaults to 0.

## sec

```python
sec(expr: Expression) -> Expression
```

The elementwise secant of a numeric expression.

## sign

```python
sign(expr: Expression) -> Expression
```

The sign of a numeric expression.

## sin

```python
sin(expr: Expression) -> Expression
```

The elementwise sine of a numeric expression.

## sinh

```python
sinh(expr: Expression) -> Expression
```

The elementwise hyperbolic sine of a numeric expression.

## sqrt

```python
sqrt(expr: Expression) -> Expression
```

The square root of a numeric expression.

## tan

```python
tan(expr: Expression) -> Expression
```

The elementwise tangent of a numeric expression.

## tanh

```python
tanh(expr: Expression) -> Expression
```

The elementwise hyperbolic tangent of a numeric expression.
