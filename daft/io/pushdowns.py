from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import singledispatchmethod
from typing import Generic, Sequence, TypeVar, Union

Value = Union[str, int, float, bool, None]
R = TypeVar("R")
C = TypeVar("C")


@dataclass(frozen=True)
class Pushdowns(ABC):
    """Base class for pushdown information."""

    pass


@dataclass(frozen=True)
class Term(ABC):
    """Term is the base class for representing expressions in pushdowns."""

    def __str__(self) -> str:
        return LispyVisitor().visit(self, None)


@dataclass(frozen=True)
class Reference(Term):
    """Reference to a field in some schema.

    Attributes:
        path: Field path in the schema which is typically a column name.
        index: Field index if this reference is bound to a schema.
    """

    path: str
    index: int | None = None


@dataclass(frozen=True)
class Literal(Term):
    """Literal value which does not currently carry its daft type information.

    The representation of types and the full-fidelity of values complicates both
    the s-expression encoding and the end user consumption. The goal here is for
    simple consumption of daft's rust expressions from a python context. We may
    iterate on this design's complexity as more advanced use-cases emerge.

    Attributes:
        value: Literal value is some python object.

    Examples:
        >>> v1 = Literal("hello")
        >>> v2 = Literal(1)
        >>> v3 = Literal(2.0)
        >>> v4 = Literal(False)
        >>> v5 = Literal(None)
    """

    value: Value


@dataclass(frozen=True)
class Expr(Term):
    """Expr is a generic expression represented by a procedure symbol and its args.

    This representation was chosen because it's the simplest while avoiding the complexity
    of a more 'pure' concatenation style or introducing additional Term variants.
    This is a trade-off where lower representation complexity requires slightly more creativity
    in representing the various expression forms, but named arguments ease consumption.

    Exprs follow the common rule that positional come before named.
    This is enforced on instantiation since we use python args and kwargs.

    Example:
        >>> Expr("f", 42)  # (f 42)
        >>> Expr("f", "hello")  # (f "hello")
        >>> Expr("f", 1, 2.5, "test")  # (f 1 2.50 "test")
        >>> Expr("f", value=True)  # (f value::true)
        >>> Expr("f", x=10, y=20.5)  # (f x::10 y::20.50)
    """

    proc: str
    args: list[Arg]

    def __init__(self, proc: str, *args: Term | Value, **kwargs: Term | Value):
        self_proc = proc
        self_args = []
        for arg in args:
            term = arg if isinstance(arg, Term) else Literal(arg)
            self_args.append(Arg(term))
        for label, arg in kwargs.items():
            term = arg if isinstance(arg, Term) else Literal(arg)
            self_args.append(Arg(term, label))
        object.__setattr__(self, "proc", self_proc)
        object.__setattr__(self, "args", self_args)

    def __str__(self) -> str:
        return LispyVisitor().visit(self, None)

    def __len__(self) -> int:
        return len(self.args)

    def __getitem__(self, key: int | str | slice) -> Term | Sequence[Term] | None:
        """Get an argument by index, name, or slice."""
        if isinstance(key, int):
            return self.args[key].term
        elif isinstance(key, str):
            return next((arg.term for arg in self.args if arg.label == key), None)
        elif isinstance(key, slice):
            return [arg.term for arg in self.args[key]]
        else:
            raise TypeError("Expected key to be int, str, or slice.")


@dataclass(frozen=True)
class Arg:
    """Arg is just an s-expression with optional label, notably does not inherit from Term.

    Attributes:
        term: Term is the actual argument.
        label: Label is an optional label if this argument was named.
    """

    term: Term
    label: str | None = None


class TermVisitor(ABC, Generic[C, R]):
    """TermVisitor uses the @singledispatchmethod for a class-based visitor.

    Note that we are not using the typical "accept" method on an term variant
    for dispatching because the @singledispatchmethod handles this for us.
    There is no need to add accept methods to each variant which simplifies
    both the term tree and the visitor implementations.

    The type `R` represents the return type, and `C` represents the context.
    The context parameter is useful when passing state which is scoped when
    performing a visitor traversal (fold). For non-scoped state, you can just
    add normal instance properties.
    """

    @singledispatchmethod
    def visit(self, term: Term, context: C) -> R:
        raise NotImplementedError(f"No visit method for type {type(term)}")

    @visit.register
    def _(self, term: Reference, context: C) -> R:
        return self.visit_reference(term, context)

    @visit.register
    def _(self, term: Literal, context: C) -> R:
        return self.visit_literal(term, context)

    @visit.register
    def _(self, term: Expr, context: C) -> R:
        return self.visit_expr(term, context)

    @abstractmethod
    def visit_reference(self, term: Reference, context: C) -> R: ...

    @abstractmethod
    def visit_literal(self, term: Literal, context: C) -> R: ...

    @abstractmethod
    def visit_expr(self, term: Expr, context: C) -> R: ...


class LispyVisitor(TermVisitor[None, str]):
    """LispyVisitor is an example visitor implementation for printing s-expressions.

    This can be implemented *much* more concisely directly in the __str__ .. but
    this is an exercise and tutorial to show off visitor usage and patterns. We
    use a 'str' return type and there is currently no scoped context. An example
    of scoped context here might be an indentation level for pretty-printing.

    An interesting exercise would be implementing https://norvig.com/lispy.html.

    Example:
    >>> visitor = LispyVisitor()
    >>> term = Expr("+", 1, 2)
    >>> print(visitor.visit(term))
    """

    ###
    # visitor variants
    ###

    def visit_reference(self, term: Reference, context: None) -> str:
        """References just use their unquoted path."""
        return term.path

    def visit_literal(self, term: Literal, context: None) -> str:
        """Literals uses the underlying value's lisp representation."""
        return self._value(term.value)

    def visit_expr(self, term: Expr, context: None) -> str:
        """Expr is represented as procs, so (proc args...)."""
        proc = term.proc
        args = ""
        for arg in term.args:
            args += " "  # no join, since we may add indentation
            args += self._arg(arg, context)
        return f"({proc}{args})" if args else f"({proc})"

    ###
    # helpers
    ###

    @singledispatchmethod
    def _value(self, value: Value | None) -> str:
        """Lisp value representation variants."""
        raise NotImplementedError

    @_value.register
    def _nil(self, _: None) -> str:
        """Lisp uses nil, but sicp scheme prefers () :shrug: .. using null."""
        return "null"

    @_value.register
    def _int(self, value: int) -> str:
        return str(value)

    @_value.register
    def _float(self, value: float) -> str:
        """Use two decimal places since precision doesn't actually matter here."""
        return f"{value:.2f}"

    @_value.register
    def _bool(self, value: bool) -> str:
        return "true" if value else "false"

    @_value.register
    def _str(self, value: str) -> str:
        """Lisp uses double-quotes for string literals."""
        return f'"{value}"'

    def _arg(self, arg: Arg, context: None) -> str:
        """Use annotation style labels e.g. label::term."""
        term: str = self.visit(arg.term, context)
        return f"{arg.label}::{term}" if arg.label else term
