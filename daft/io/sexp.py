from __future__ import annotations

from abc import ABC


from dataclasses import dataclass
from functools import singledispatchmethod
from typing import Generic, TypeVar

Value = str | int | float | bool
R = TypeVar("R")
C = TypeVar("C")


@dataclass(frozen=True)
class Pushdowns(ABC):
    """Marker interface for pushdown objects."""
    pass


@dataclass(frozen=True)
class Sexp(ABC):
    """Sexp is the base class for representing daft's rust expressions."""
    pass


@dataclass(frozen=True)
class Atom(Sexp):
    """Atom is a value and does not carry any daft type information.

    The representation of types and the full-fidelity of values complicates both
    the s-expression encoding and the end user consumption. The goal here is for
    simple consumption of daft's rust expressions from a python context. We may
    iterate on this design's complexity as more advanced use-cases emerge.

    Example:
    >>> v1 = Atom("hello")
    >>> v2 = Atom(1)
    >>> v3 = Atom(2.0)
    >>> v4 = Atom(False)
    """
    value: Value | None

    def __repr__(self) -> str:
        return ReprVisitor().visit(self, None)


@dataclass(frozen=True)
class Arg:
    """Arg is just an s-expression with optional label, notably does not inherit from Sexp."""
    sexp: Sexp
    label: str | None = None

    def __repr__(self) -> str:
        return ReprVisitor()._arg(self, None)


@dataclass(frozen=True)
class Expr(Sexp):
    """Expr is a generic expression form which models all as a 'call' with args.
    
    This representation was chosen because it's the simplest while avoiding the complexity
    of a more 'pure' concatentation style or introducing additional Sexp variants.
    This is a trade-off where lower representation complexity equires slightly more creativity
    in representing the various expression forms, but named arguments ease consumption.

    Each expression's arguments are named and follow the common rule that positional
    come before named. This is enforced on instantiation since we use python args and kwargs.

    Example:
    >>> Expr("a", 42)               # (a 42)
    >>> Expr("b", "hello")          # (b "hello")
    >>> Expr("c", 1, 2.5, "test")   # (c 1 2.50 "test")
    >>> Expr("d", value=True)       # (d value::true)
    >>> Expr("e", x=10, y=20.5)     # (e x::10 y::20.50)
    """
    symbol: str
    args: list[Arg]

    def __init__(self, symbol: str, *args: Sexp | Value, **kwargs: Sexp | Value):
        self_symbol = symbol
        self_args = []
        for arg in args:
            sexp = arg if isinstance(arg, Sexp) else Atom(arg)
            self_args.append(Arg(sexp))
        for label, arg in kwargs.items():
            sexp = arg if isinstance(arg, Sexp) else Atom(arg)
            self_args.append(Arg(sexp, label))
        object.__setattr__(self, 'symbol', self_symbol)
        object.__setattr__(self, 'args', self_args)

    def __repr__(self) -> str:
        return ReprVisitor().visit(self, None)


class SexpVisitor(Generic[C, R], ABC):
    """SexprVisitor uses the @singledispatchmethod for a class-based visitor.
    
    Note that we are not using the typical "accept" method on an sexp variant
    for dispatching because the @singledispatchmethod handles this for us.
    There is no need to add accept methods to each variant which simplifies
    both the sexp tree and the visitor implementations.

    The type `R` represents the return type, and `C` represents the context.
    The context parameter is useful when passing state which is scoped when
    performing a visitor traversal (fold). For non-scoped state, you can just
    add normal instance properties.
    """
    @singledispatchmethod
    def visit(self, sexp: Sexp, context: C) -> R:
        raise NotImplementedError(f"No visit method for type {type(sexp)}")


class ReprVisitor(SexpVisitor[None, str]):
    """ReprVisitor is an example visitor implementation for printing s-expressions.
    
    This could be implemented *much* more concisely directly in the repr .. but
    this is an exercise and tutorial to show off visitor usage and patterns. We
    use a 'str' return type and there is currently no scoped context. An example
    of scoped context here might be an indentation level for pretty-printing.

    Example:
    >>> visitor = ReprVisitor()
    >>> sexp = Expr("+", 1, 2)
    >>> print(visitor.visit(sexp))
    """

    ###
    # visitor variant
    ###

    @SexpVisitor.visit.register
    def _(self, sexp: Atom, context: None) -> str:
        """Atom uses the underlying value's lisp representation."""
        return self._value(sexp.value)

    @SexpVisitor.visit.register
    def _(self, sexp: Expr, context: None) -> str:
        """Expr is represented as (symbol args...)."""
        symbol = sexp.symbol
        args = ""
        for arg in sexp.args:
            args += " " # no join, since we may add indentation
            args += self._arg(arg, context)
        return f"({symbol}{args})" if args else f"({symbol})"

    ###
    # helpers
    ###

    @singledispatchmethod
    def _value(self, value: Value | None) -> str:
        """lisp value representation variants."""
        raise NotImplementedError

    @_value.register
    def _nil(self, _: None) -> str:
        """lisp uses nil, but sicp scheme prefers () :shrug:"""
        return "nil"

    @_value.register
    def _int(self, value: int) -> str:
        return str(value)

    @_value.register
    def _float(self, value: float) -> str:
        """use two decimal places since precision doesn't actually matter here."""
        return f"{value:.2f}"

    @_value.register
    def _bool(self, value: bool) -> str:
        return "true" if value else "false"

    @_value.register
    def _str(self, value: str) -> str:
        """lisp uses double-quotes for string literals."""
        return f'"{value}"'

    def _arg(self, arg: Arg, context: None) -> str:
        """Arg is uses annotation style labels e.g. label::sexp"""
        sexp = self.visit(arg.sexp, context)
        return f"{arg.label}::{sexp}" if arg.label else sexp
