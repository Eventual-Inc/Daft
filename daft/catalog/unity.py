"""DEPRECATED: Please use `Catalog.from_unity`, these APIs will be removed in the next release."""

from __future__ import annotations

from daft.catalog.__unity import UnityCatalog, UnityTable

# TODO deprecated catalog APIs #3819
UnityCatalogAdaptor = UnityCatalog

# TODO deprecated catalog APIs #3819
UnityTableAdaptor = UnityTable
