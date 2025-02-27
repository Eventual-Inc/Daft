"""DEPRECATED: Please use `Catalog.from_iceberg`, these APIs will be removed in the next release."""

from __future__ import annotations

from daft.catalog.__iceberg import IcebergCatalog, IcebergTable

# TODO deprecated catalog APIs #3819
PyIcebergCatalogAdaptor = IcebergCatalog

# TODO deprecated catalog APIs #3819
PyIcebergTableAdaptor = IcebergTable
