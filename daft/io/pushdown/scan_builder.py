from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from daft.daft import PyPushdowns

from .pushdowns import wrap_scan_operator

if TYPE_CHECKING:
    from ..scan import ScanOperator


class ScanBuilder(abc.ABC):
    @abc.abstractmethod
    def build(self, scan_operator: ScanOperator, pushdowns: PyPushdowns | None = None) -> ScanOperator:
        pass


class ScanBuilderImpl(ScanBuilder):
    def __init__(self) -> None:
        self._pushdowns = None

    def build(self, scan_operator: ScanOperator, pushdowns: PyPushdowns | None = None) -> ScanOperator:
        actual_pushdowns = pushdowns or PyPushdowns(filters=None, limit=None, columns=None, partition_filters=None)
        return wrap_scan_operator(scan_operator, actual_pushdowns)
