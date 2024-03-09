# isort: dont-add-import: from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class DataCatalogType(Enum):
    """Supported data catalogs."""

    GLUE = "glue"
    """
    AWS Glue Data Catalog.
    """
    UNITY = "unity"
    """
    Databricks Unity Catalog.
    """


@dataclass
class DataCatalogTable:
    """
    A reference to a table in some database in some data catalog.

    See :class:`~.DataCatalog`
    """

    catalog: DataCatalogType
    database_name: str
    table_name: str
    catalog_id: Optional[str] = None
