from __future__ import annotations

from daft.daft import PyPartitionField, PyPartitionTransform
from daft.schema import Field


class PartitionField:
    """Partitioning Field of a Scan Source such as Hive or Iceberg."""

    _partition_field: PyPartitionField

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a PartitionField via __init__ ")

    def __repr__(self) -> str:
        return self._partition_field.__repr__()

    @classmethod
    def create(
        cls,
        field: Field,
        source_field: Field | None = None,
        transform: PartitionTransform | None = None,
    ) -> PartitionField:
        pf = cls.__new__(cls)
        pf._partition_field = PyPartitionField(
            field._field,
            source_field._field if source_field is not None else None,
            transform._partition_transform if transform is not None else None,
        )
        return pf

    @property
    def field(self) -> Field:
        return Field._from_pyfield(self._partition_field.field)

    @property
    def source_field(self) -> Field | None:
        if source_field := self._partition_field.source_field:
            return Field._from_pyfield(source_field)
        else:
            return None

    @property
    def transform(self) -> PartitionTransform | None:
        if transform := self._partition_field.transform:
            return PartitionTransform._from_py_partition_transform(transform)
        else:
            return None


class PartitionTransform:
    """Partitioning Transform from a Data Catalog source field to a Partitioning Columns."""

    _partition_transform: PyPartitionTransform

    def __init__(self) -> None:
        raise ValueError(
            "We do not support creating a PartitionTransform directly, please use one of the factory methods."
        )

    def __repr__(self) -> str:
        return self._partition_transform.__repr__()

    def __eq__(self, value: object) -> bool:
        if isinstance(value, PartitionTransform):
            return self._partition_transform == value._partition_transform
        return False

    @classmethod
    def _from_py_partition_transform(cls, py_partition_transform: PyPartitionTransform) -> PartitionTransform:
        pt = cls.__new__(cls)
        pt._partition_transform = py_partition_transform
        return pt

    @classmethod
    def identity(cls) -> PartitionTransform:
        """Creates an identity partition transform that returns the input value unchanged.

        Returns:
            PartitionTransform: A new identity partition transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.identity()
            >>> assert transform.is_identity()
        """
        pt = PyPartitionTransform.identity()
        return cls._from_py_partition_transform(pt)

    @classmethod
    def year(cls) -> PartitionTransform:
        """Creates a partition transform that extracts the year from a timestamp.

        Returns:
            PartitionTransform: A new year partition transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.year()
            >>> assert transform.is_year()
        """
        pt = PyPartitionTransform.year()
        return cls._from_py_partition_transform(pt)

    @classmethod
    def month(cls) -> PartitionTransform:
        """Creates a partition transform that extracts the month from a timestamp.

        Returns:
            PartitionTransform: A new month partition transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.month()
            >>> assert transform.is_month()
        """
        pt = PyPartitionTransform.month()
        return cls._from_py_partition_transform(pt)

    @classmethod
    def day(cls) -> PartitionTransform:
        """Creates a partition transform that extracts the day from a timestamp.

        Returns:
            PartitionTransform: A new day partition transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.day()
            >>> assert transform.is_day()
        """
        pt = PyPartitionTransform.day()
        return cls._from_py_partition_transform(pt)

    @classmethod
    def hour(cls) -> PartitionTransform:
        """Creates a partition transform that extracts the hour from a timestamp.

        Returns:
            PartitionTransform: A new hour partition transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.hour()
            >>> assert transform.is_hour()
        """
        pt = PyPartitionTransform.hour()
        return cls._from_py_partition_transform(pt)

    @classmethod
    def iceberg_bucket(cls, n: int) -> PartitionTransform:
        """Creates an Iceberg bucket partition transform that hashes values into n buckets.

        Args:
            n: Number of buckets to hash values into.

        Returns:
            PartitionTransform: A new Iceberg bucket partition transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.iceberg_bucket(10)
            >>> assert transform.is_iceberg_bucket()
            >>> assert transform.num_buckets == 10
        """
        pt = PyPartitionTransform.iceberg_bucket(n)
        return cls._from_py_partition_transform(pt)

    @classmethod
    def iceberg_truncate(cls, w: int) -> PartitionTransform:
        """Creates an Iceberg truncate partition transform that truncates values to width w.

        Args:
            w: Width to truncate values to.

        Returns:
            PartitionTransform: A new Iceberg truncate partition transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.iceberg_truncate(10)
            >>> assert transform.is_iceberg_truncate()
            >>> assert transform.width == 10
        """
        pt = PyPartitionTransform.iceberg_truncate(w)
        return cls._from_py_partition_transform(pt)

    def is_identity(self) -> bool:
        """Checks if this is an identity partition transform.

        Returns:
            bool: True if this is an identity transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.identity()
            >>> assert transform.is_identity()
        """
        return self._partition_transform.is_identity()

    def is_year(self) -> bool:
        """Checks if this is a year partition transform.

        Returns:
            bool: True if this is a year transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.year()
            >>> assert transform.is_year()
        """
        return self._partition_transform.is_year()

    def is_month(self) -> bool:
        """Checks if this is a month partition transform.

        Returns:
            bool: True if this is a month transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.month()
            >>> assert transform.is_month()
        """
        return self._partition_transform.is_month()

    def is_day(self) -> bool:
        """Checks if this is a day partition transform.

        Returns:
            bool: True if this is a day transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.day()
            >>> assert transform.is_day()
        """
        return self._partition_transform.is_day()

    def is_hour(self) -> bool:
        """Checks if this is an hour partition transform.

        Returns:
            bool: True if this is an hour transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.hour()
            >>> assert transform.is_hour()
        """
        return self._partition_transform.is_hour()

    def is_iceberg_bucket(self) -> bool:
        """Checks if this is an Iceberg bucket partition transform.

        Returns:
            bool: True if this is an Iceberg bucket transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.iceberg_bucket(10)
            >>> assert transform.is_iceberg_bucket()
        """
        return self._partition_transform.is_iceberg_bucket()

    def is_iceberg_truncate(self) -> bool:
        """Checks if this is an Iceberg truncate partition transform.

        Returns:
            bool: True if this is an Iceberg truncate transform.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.iceberg_truncate(10)
            >>> assert transform.is_iceberg_truncate()
        """
        return self._partition_transform.is_iceberg_truncate()

    @property
    def num_buckets(self) -> int:
        """If this is an iceberg_bucket partition transform, then returns the number of buckets; otherwise an attribute error is raised.

        Returns:
            int: Number of buckets.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.iceberg_bucket(10)
            >>> assert transform.num_buckets == 10
        """
        return self._partition_transform.num_buckets()

    @property
    def width(self) -> int:
        """If this is an iceberg_truncate partition transform, then returns the truncation width; otherwise an attribute error is raised.

        Returns:
            int: Truncation width.

        Examples:
            >>> from daft.io.partitioning import PartitionTransform
            >>> transform = PartitionTransform.iceberg_truncate(10)
            >>> assert transform.width == 10
        """
        return self._partition_transform.width()
