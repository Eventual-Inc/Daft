# from __future__ import annotations

# from typing import List

# from tabulate import tabulate

# from daft.column import Column, ColumnArgType, ColumnExpression, ColumnType
# from daft.operations import Operation


# class RowView:
#     __slots__ = ["_index", "_df"]

#     def __init__(self, df: DataFrame, index: int) -> None:
#         self._index = index
#         self._df = df
#         if index >= df.count():
#             raise ValueError("index out of bounds")

#     def __repr__(self) -> str:
#         df = self.__getattribute__("_df")
#         index = self.__getattribute__("_index")
#         return f"RowView: index: {index}\n" + tabulate([df.columns, [df._data[col][index] for col in df.columns]])

#     def __dir__(self):
#         return self.__getattribute__("_df").columns

#     def __getattr__(self, col: str):
#         df = self.__getattribute__("_df")
#         index = self.__getattribute__("_index")
#         if col not in df.columns:
#             raise ValueError(f"column {col} not in {df.columns}")
#         return df._data[col][index]


# class DataFrame:
#     def __init__(self, cols: List[ColumnArgType]) -> None:
#         self._cols = [Column.from_arg(c) for c in self._cols]
#         self._column_names = [c.name for c in self._cols]

#     @property
#     def columns(self) -> List[str]:
#         return self._column_names

#     # def __getitem__(self, index: int) -> RowView:
#     #     return DataFrame.RowView(self, index)

#     def select(self, *columns: ColumnArgType) -> DataFrame:
#         selected_cols = []
#         for c in columns:
#             if isinstance(c, Column):
#                 selected_cols.append(c)
#             elif isinstance(c, str):
#                 assert c in self.columns, f"{c} not found in dataframes columns: {self.columns}"
#                 index = self.columns.index(c)
#                 selected_cols.append(self._cols[index])
#         return DataFrame(selected_cols)

#     def with_column(self, name: str, col: Column) -> Column:
#         assert isinstance(col, Column), "col must be Column type"
#         assert name not in self.columns, f"duplicate column name {name}"
#         return DataFrame(self._cols + [col.alias(name)])

#     def where(self, expr: ColumnExpression) -> DataFrame:
#         new_columns = [
#             Column(
#                 name=c.name,
#                 column_type=ColumnType.RESULT,
#                 operation=Operation("where_pl", [c]),
#             )
#             for c in self._cols
#         ]
#         return DataFrame(new_columns)

#     def limit(self, num: int) -> DataFrame:
#         new_columns = [
#             Column(
#                 name=c.name,
#                 column_type=ColumnType.RESULT,
#                 operation=Operation("limit_pl", [c]),
#             )
#             for c in self._cols
#         ]
#         return DataFrame(new_columns)
