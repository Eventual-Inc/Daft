from __future__ import annotations

from daft.daft import PyGeospatialMode as _PyGeospatialMode


class GeospatialMode:
    """Geospatial mode describing the dimension, coordinate layout, and optional CRS/edges metadata.

    Args:
        dimension: Coordinate dimensions. One of ``"xy"``, ``"xyz"``, ``"xym"``, ``"xyzm"``. Defaults to ``"xy"``.
        coord_type: Coordinate storage layout. One of ``"interleaved"`` or ``"separated"``. Defaults to ``"separated"``.
        crs: Optional CRS string (plain authority code like ``"EPSG:4326"`` or JSON).
        crs_type: Optional CRS encoding. One of ``"projjson"``, ``"wkt2:2019"``, ``"authority_code"``, ``"srid"``.
        edges: Optional edge interpolation algorithm. One of ``"andoyer"``, ``"karney"``, ``"spherical"``, ``"thomas"``, ``"vincenty"``.

    Examples:
        >>> mode = GeospatialMode()
        >>> mode = GeospatialMode("xyz", "separated")
        >>> mode = GeospatialMode("xy", "interleaved", crs="EPSG:4326", crs_type="authority_code", edges="spherical")
        >>> # A single mode can be reused across multiple DataType constructors
        >>> mode = GeospatialMode("xyz", "interleaved")
        >>> point_type = DataType.point(mode)
        >>> line_type = DataType.linestring(mode)
    """

    def __init__(
        self,
        dimension: str = "xy",
        coord_type: str = "separated",
        *,
        crs: str | None = None,
        crs_type: str | None = None,
        edges: str | None = None,
    ) -> None:
        if coord_type != "separated":
            raise NotImplementedError("Only separated coordinate type is supported")

        self._inner = _PyGeospatialMode.from_user_defined_mode(dimension, coord_type, crs, crs_type, edges)

    def __repr__(self) -> str:
        return repr(self._inner)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GeospatialMode):
            return NotImplemented
        return self._inner == other._inner

    def __hash__(self) -> int:
        return hash(self._inner)
