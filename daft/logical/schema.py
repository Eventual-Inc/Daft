from __future__ import annotations

from typing import Iterator, TypeVar

from daft.expressions import Expression, ExpressionList, col

ExpressionType = TypeVar("ExpressionType", bound=Expression)

from daft.logical.field import Field


class Schema:
    fields: dict[str, Field]

    def __init__(self, fields: list[Field]) -> None:
        self.fields = {}
        for field in fields:
            assert isinstance(field, Field), f"expect {Field}, got {type(field)}"
            assert field.name not in self.fields
            self.fields[field.name] = field

    def __getitem__(self, key: str) -> Field:
        if key not in self.fields:
            raise ValueError(f"{key} was not found in Schema of fields {self.fields.keys()}")
        return self.fields[key]

    def __len__(self) -> int:
        return len(self.fields)

    def __iter__(self) -> Iterator[Field]:
        return iter(self.fields.values())

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Schema) and self.fields == other.fields

    def column_names(self) -> list[str]:
        return list(self.fields.keys())

    def to_name_set(self) -> set[str]:
        return set(self.column_names())

    def __repr__(self) -> str:
        return repr([(field.name, field.dtype) for field in self.fields.values()])

    def _repr_html_(self) -> str:
        return repr([(field.name, field.dtype) for field in self.fields.values()])

    def to_column_expressions(self) -> ExpressionList:
        return ExpressionList([col(f.name) for f in self.fields.values()])

    def union(self, other: Schema) -> Schema:
        assert isinstance(other, Schema), f"expected Schema, got {type(other)}"
        seen = {}
        for f in self.fields.values():
            assert f.name not in seen
            seen[f.name] = f

        for f in other.fields.values():
            assert f.name not in seen
            seen[f.name] = f

        return Schema([f for f in seen.values()])


# class ExpressionList(Iterable[ExpressionType]):
#     def __init__(self, exprs: list[ExpressionType]) -> None:
#         self.exprs = copy.deepcopy(exprs)
#         self.names: list[str] = []
#         name_set = set()
#         for i, e in enumerate(exprs):
#             e_name = e.name()
#             if e_name is None:
#                 e_name = "col_{i}"
#             if e_name in name_set:
#                 raise ValueError(f"duplicate name found {e_name}")
#             self.names.append(e_name)
#             name_set.add(e_name)

#     def __len__(self) -> int:
#         return len(self.exprs)

#     def contains(self, column: ColumnExpression) -> bool:
#         for e in self.exprs:
#             if e.is_eq(column):
#                 return True
#         return False

#     def get_expression_by_name(self, name: str) -> ExpressionType | None:
#         for i, n in enumerate(self.names):
#             if n == name:
#                 return self.exprs[i]
#         return None

#     def get_expression_by_id(self, id: int) -> ExpressionType | None:
#         for e in self.exprs:
#             if e.name() == id:
#                 return e
#         return None

#     def resolve(self, input_schema: ExpressionList | None = None) -> ExpressionList:
#         if input_schema is None:
#             for e in self.exprs:
#                 assert isinstance(e, ColumnExpression), "we can only resolve ColumnExpression without an input_schema"
#         else:
#             for e in self.exprs:
#                 for col_expr in e.required_columns():
#                     col_expr_name = col_expr.name()
#                     assert col_expr_name is not None
#                     match_output_expr = input_schema.get_expression_by_name(col_expr_name)
#                     if match_output_expr is not None:
#                         col_expr.resolve_to_expression(match_output_expr)
#                     else:
#                         raise ValueError(f"Could not find expr by name {col_expr_name}")
#                 e.resolved_type()
#         return self

#     def unresolve(self) -> ExpressionList:
#         return copy.deepcopy(self)

#     def keep(self, to_keep: list[str]) -> ExpressionList:
#         # is_resolved = True
#         exprs_to_keep: list[Expression] = []
#         for name in to_keep:
#             expr = self.get_expression_by_name(name)
#             if expr is None:
#                 raise ValueError(f"{name} not found in {self.names}")
#             exprs_to_keep.append(expr)
#         return ExpressionList(exprs_to_keep)

#     def __repr__(self) -> str:
#         return repr(self.exprs)

#     def union(
#         self, other: ExpressionList, rename_dup: str | None = None, other_override: bool = False
#     ) -> ExpressionList:
#         """Unions two schemas together

#         Note: only one of rename_dup or other_override can be specified as the behavior when resolving naming conflicts.

#         Args:
#             other (ExpressionList): other ExpressionList to union with this one
#             rename_dup (Optional[str], optional): when conflicts in naming happen, append this string to the conflicting column in `other`. Defaults to None.
#             other_override (bool, optional): when conflicts in naming happen, use the `other` column instead of `self`. Defaults to False.
#         """
#         assert not ((rename_dup is not None) and other_override), "Only can specify one of rename_dup or other_override"
#         deduped = self.exprs.copy()
#         seen: dict[str, ExpressionType] = {}
#         for e in self.exprs:
#             name = e.name()
#             if name is not None:
#                 seen[name] = e

#         for e in other:
#             name = e.name()
#             assert name is not None

#             if name in seen:
#                 if rename_dup is not None:
#                     name = f"{rename_dup}{name}"
#                     e = cast(ExpressionType, e.alias(name))
#                 elif other_override:
#                     # Allow this expression in `other` to override the existing expressions
#                     deduped = [current_expr for current_expr in deduped if current_expr.name() != name]
#                 else:
#                     raise ValueError(
#                         f"Duplicate name found with different expression. name: {name}, seen: {seen[name]}, current: {e}"
#                     )
#             deduped.append(e)
#             seen[name] = e

#         return ExpressionList(deduped)

#     def to_column_expressions(self) -> ExpressionList:
#         return ExpressionList([e.to_column_expression() for e in self.exprs])

#     def __eq__(self, other: object) -> bool:
#         if not isinstance(other, ExpressionList):
#             return False

#         return len(self.exprs) == len(other.exprs) and all(
#             (s.name() == o.name()) and (s.resolved_type()) == o.resolved_type() for s, o in zip(self.exprs, other.exprs)
#         )

#     def required_columns(self) -> ExpressionList:
#         name_to_expr: dict[str, ColumnExpression] = {}
#         for e in self.exprs:
#             for c in e.required_columns():
#                 name = c.name()
#                 assert name is not None
#                 name_to_expr[name] = c
#         return ExpressionList([e for e in name_to_expr.values()])

#     def input_mapping(self) -> dict[str, str]:
#         result = {}
#         for e in self.exprs:
#             input_map = e._input_mapping()
#             if input_map is not None:
#                 result[e.name()] = input_map
#         return result

#     def __iter__(self) -> Iterator[ExpressionType]:
#         return iter(self.exprs)

#     def to_name_set(self) -> set[str]:
#         id_set = set()
#         for c in self:
#             name = c.name()
#             assert name is not None
#             id_set.add(name)
#         return id_set

#     def resource_request(self) -> ResourceRequest:
#         """Returns the requested resources for the execution of all expressions in this list"""
#         return ResourceRequest.max_resources([e.resource_request() for e in self.exprs])
