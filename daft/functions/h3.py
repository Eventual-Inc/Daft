"""H3 Geospatial Indexing Functions.

Functions for [H3](https://h3geo.org/), Uber's hierarchical hexagonal geospatial indexing system.
"""

from __future__ import annotations

from daft.expressions import Expression


def h3_latlng_to_cell(lat: Expression, lng: Expression, resolution: int) -> Expression:
    """Converts latitude/longitude coordinates to an H3 cell index at the given resolution (0-15)."""
    return Expression._call_builtin_scalar_fn("h3_latlng_to_cell", lat, lng, resolution=resolution)


def h3_cell_to_lat(cell: Expression) -> Expression:
    """Returns the latitude of the center of an H3 cell.

    Args:
        cell: H3 cell index (UInt64 or Utf8 hex string).
    """
    return Expression._call_builtin_scalar_fn("h3_cell_to_lat", cell)


def h3_cell_to_lng(cell: Expression) -> Expression:
    """Returns the longitude of the center of an H3 cell.

    Args:
        cell: H3 cell index (UInt64 or Utf8 hex string).
    """
    return Expression._call_builtin_scalar_fn("h3_cell_to_lng", cell)


def h3_cell_to_str(cell: Expression) -> Expression:
    """Converts an H3 cell index to its hex string representation.

    Args:
        cell: H3 cell index (UInt64 or Utf8 hex string).
    """
    return Expression._call_builtin_scalar_fn("h3_cell_to_str", cell)


def h3_str_to_cell(hex: Expression) -> Expression:
    """Converts an H3 hex string to a cell index (UInt64).

    For maximum throughput, convert to UInt64 once at the top of a pipeline
    and operate on integers throughout, rather than passing hex strings to
    every function call.
    """
    return Expression._call_builtin_scalar_fn("h3_str_to_cell", hex)


def h3_cell_resolution(cell: Expression) -> Expression:
    """Returns the resolution (0-15) of an H3 cell index.

    Args:
        cell: H3 cell index (UInt64 or Utf8 hex string).
    """
    return Expression._call_builtin_scalar_fn("h3_cell_resolution", cell)


def h3_cell_is_valid(cell: Expression) -> Expression:
    """Returns true if the value is a valid H3 cell index.

    Args:
        cell: H3 cell index (UInt64 or Utf8 hex string).
    """
    return Expression._call_builtin_scalar_fn("h3_cell_is_valid", cell)


def h3_cell_parent(cell: Expression, resolution: int) -> Expression:
    """Returns the parent cell index at the given resolution.

    The output type matches the input type: string in, string out.

    Args:
        cell: H3 cell index (UInt64 or Utf8 hex string).
        resolution: Target resolution (0-15). Must be coarser (lower) than the cell's resolution.
    """
    return Expression._call_builtin_scalar_fn("h3_cell_parent", cell, resolution=resolution)


def h3_grid_distance(a: Expression, b: Expression) -> Expression:
    """Returns the grid distance (number of H3 cells) between two cells.

    Returns null if cells are not comparable (different resolutions or pentagonal distortion).

    Args:
        a: H3 cell index (UInt64 or Utf8 hex string).
        b: H3 cell index (UInt64 or Utf8 hex string).
    """
    return Expression._call_builtin_scalar_fn("h3_grid_distance", a, b)
