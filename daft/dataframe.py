from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass

from typing import Any, Callable, Dict, List, Union

from tabulate import tabulate

@dataclass
class Column:
    name: str


@dataclass
class QueryExpression:
    ...

ColumnArgType = Union[Column, str]

class DataFrame:

    class RowView:
        __slots__ = ["_index", "_df"]
        def __init__(self, df: DataFrame, index: int) -> None:
            self._index = index
            self._df = df
            if index >= df.count():
                raise ValueError("index out of bounds")

        def __repr__(self) -> str:
            df = self.__getattribute__("_df")
            index = self.__getattribute__("_index")
            return f"RowView: index: {index}\n" + \
            tabulate([
                df.columns,
                [df._data[col][index] for col in df.columns]
            ])

        def __dir__(self):
            return self.__getattribute__("_df").columns

        def __getattr__(self, col: str):
            df = self.__getattribute__("_df")
            index = self.__getattribute__("_index")
            if col not in df.columns:
                raise ValueError(f"column {col} not in {df.columns}")
            return df._data[col][index]


    def __init__(self, column_to_list: Dict[str, List]) -> None:
        if len(column_to_list.keys()) == 0:
            raise ValueError("no columns passed in")
        self._data = column_to_list
        self._columns = list(column_to_list.keys())
        self._count = max(map(len, column_to_list.values()))
        min_count = min(map(len, column_to_list.values()))
        if self._count != min_count:
            raise ValueError('columns are uneven')
    
    @property
    def columns(self) -> List[str]:
        return self._columns

    @classmethod
    def from_items(cls, data: List[Dict[str, Any]]) -> DataFrame:
        transposed = defaultdict(list)
        for row in data:
            for k, v in row.items():
                transposed[k].append(v)
            leftover_keys = transposed.keys() - row.keys()
            for k in leftover_keys:
                transposed[k].append(None)
        return DataFrame(transposed)

    def __getitem__(self, index: int) -> RowView:
        return DataFrame.RowView(self, index)


    def __norm_columns(self, *columns: ColumnArgType) -> List[Column]:
        norm_columns = [Column(name=c) if isinstance(c, str) else c for c in columns]
        return norm_columns


    def select(self, *columns: ColumnArgType) -> DataFrame:
        norm_columns = self.__norm_columns(*columns)
        column_names = [c.name for c in norm_columns]
        if not all(c in self._columns for c in column_names):
            raise ValueError(f"Column not in DataFrame {set(column_names) - set(self._columns)}")
        return DataFrame({k: self._data[k].copy() for k in column_names})


    def count(self) -> int:
        return self._count

    def with_column(self, name: str, new_col: ColumnArgType) -> Column:
        ...
    
    def where(self, expr: QueryExpression) -> DataFrame:
        ...
    
    def sort(self, *columns: ColumnArgType) -> DataFrame:
        ...