from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import ClusteringSpec as _PyClusteringSpec
from daft.expressions import Expression, col

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = [
    "ClusteringSpec",
]


class ClusteringSpec:
    """Declares how a custom ``DataSource``'s output is distributed at execution time.

    A ``DataSource`` returns a ``ClusteringSpec`` from
    :meth:`DataSource.get_clustering_spec <daft.io.source.DataSource.get_clustering_spec>`
    to tell the optimizer that its output is already hash-partitioned by some keys. When the
    keys a downstream ``groupby`` / ``Window.partition_by`` / ``distinct`` requires are covered
    by the declared clustering, the optimizer skips the shuffle it would otherwise insert.

    Keys may be column names or arbitrary :class:`~daft.expressions.Expression` values. Use the
    same expression on both the declaration and the downstream operator so they compare equal.

    Warning:
        This API is early in its development and is subject to change.
    """

    _spec: _PyClusteringSpec

    def __init__(self) -> None:
        raise NotImplementedError("Use ClusteringSpec.hash(...) to construct a ClusteringSpec.")

    def __repr__(self) -> str:
        return self._spec.__repr__()

    @classmethod
    def _from_pyclusteringspec(cls, spec: _PyClusteringSpec) -> ClusteringSpec:
        out = cls.__new__(cls)
        out._spec = spec
        return out

    @staticmethod
    def hash(*cols: str | Expression) -> ClusteringSpec:
        """Declares that the source's output is hash-partitioned by ``cols``.

        Every row with the same hash of ``cols`` is guaranteed to live in the same execution
        partition. Column-name strings are interpreted as column references.

        Args:
            cols: The clustering keys, as column names or expressions.

        Examples:
            >>> from daft import col
            >>> from daft.io.clustering import ClusteringSpec
            >>> spec = ClusteringSpec.hash("producer", col("id") % 100)
        """
        exprs: Sequence[Expression] = [c if isinstance(c, Expression) else col(c) for c in cols]
        return ClusteringSpec._from_pyclusteringspec(_PyClusteringSpec.hash([e._expr for e in exprs]))
