from __future__ import annotations

from daft.expressions import Expression


def h3_latlng_to_cell(lat: Expression, lng: Expression, resolution: int) -> Expression:
    """Converts latitude/longitude coordinates to an H3 cell index at the given resolution (0-15)."""
    return Expression._call_builtin_scalar_fn("h3_latlng_to_cell", lat, lng, resolution=resolution)


def h3_cell_to_lat(cell: Expression) -> Expression:
    """Returns the latitude of the center of an H3 cell."""
    return Expression._call_builtin_scalar_fn("h3_cell_to_lat", cell)


def h3_cell_to_lng(cell: Expression) -> Expression:
    """Returns the longitude of the center of an H3 cell."""
    return Expression._call_builtin_scalar_fn("h3_cell_to_lng", cell)


def h3_cell_to_str(cell: Expression) -> Expression:
    """Converts an H3 cell index (UInt64) to its hex string representation."""
    return Expression._call_builtin_scalar_fn("h3_cell_to_str", cell)


def h3_str_to_cell(hex: Expression) -> Expression:
    """Converts an H3 hex string to a cell index (UInt64)."""
    return Expression._call_builtin_scalar_fn("h3_str_to_cell", hex)


def h3_cell_resolution(cell: Expression) -> Expression:
    """Returns the resolution (0-15) of an H3 cell index."""
    return Expression._call_builtin_scalar_fn("h3_cell_resolution", cell)


def h3_cell_is_valid(cell: Expression) -> Expression:
    """Returns true if the UInt64 value is a valid H3 cell index."""
    return Expression._call_builtin_scalar_fn("h3_cell_is_valid", cell)


def h3_cell_parent(cell: Expression, resolution: int) -> Expression:
    """Returns the parent cell index at the given resolution."""
    return Expression._call_builtin_scalar_fn("h3_cell_parent", cell, resolution=resolution)


def h3_grid_distance(a: Expression, b: Expression) -> Expression:
    """Returns the grid distance (number of H3 cells) between two cells."""
    return Expression._call_builtin_scalar_fn("h3_grid_distance", a, b)
