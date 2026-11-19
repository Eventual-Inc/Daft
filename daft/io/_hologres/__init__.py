# isort: dont-add-import: from __future__ import annotations

"""Internal Hologres integration for Daft.

This package is internal; please use:

- ``Catalog.from_hologres(...)`` for catalog operations
- ``daft.io.read_hologres(...)`` for reads
- ``HologresTable.read/append/overwrite`` for table IO

Large portions of the wire-protocol code (COPY SQL construction, PostgreSQL
binary COPY row encoding, internal stage SQL, Arrow type mapping) are adapted
from the `holo-client-py` connector (Apache-2.0, Alibaba Cloud), repository:
https://github.com/alibabacloud-iot/alibabacloud-hologres-connectors
"""
