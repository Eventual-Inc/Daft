"""SQL builders for Hologres internal stage operations (Hologres >= 4.1).

Generates SQL statements for:
- Uploading Arrow data to an internal stage via the COPY protocol
- Loading data from a stage into the target table via INSERT ... SELECT
- Creating and dropping internal stages

Adapted from holo-client-py (hologres/_stage_sql.py); operates on plain column
name/type pairs instead of holo-client Column objects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from psycopg import sql

if TYPE_CHECKING:
    from collections.abc import Sequence


def _stage_type_name(type_name: str, precision: int, scale: int) -> str:
    """Resolve the real type name for the ``external_files AS`` clause.

    Serial types become their base integer types and numerics carry explicit
    precision/scale. JSON/JSONB columns are NOT remapped here; the caller
    handles that.
    """
    name = type_name.strip().lower()
    if name == "serial":
        return "int"
    if name == "bigserial":
        return "bigint"
    if name == "smallserial":
        return "smallint"
    if name in ("numeric", "decimal"):
        return f"decimal({precision if precision > 0 else 38},{max(scale, 0)})"
    return name


def build_copy_in_stage_sql(stage_name: str, file_name: str) -> str:
    """Build the COPY SQL for uploading a file to an internal stage."""
    return f"copy external_files(path='internal_stage://{stage_name}/{file_name}') from stdin;"


def build_create_stage_sql(stage_name: str, group_name: str = "default", ttl_seconds: int = 3600) -> str:
    """Build the CALL SQL to create an internal stage."""
    return f"call hologres.hg_create_internal_stage('{stage_name}', '{group_name}', {ttl_seconds});"


def build_drop_stage_sql(stage_name: str) -> str:
    """Build the CALL SQL to drop an internal stage."""
    return f"call hologres.hg_drop_internal_stage('{stage_name}');"


def build_insert_select_from_stage_sql(
    schema_name: str,
    table_name: str,
    columns: Sequence[tuple[str, str, int, int]],
    stages: Sequence[str],
    conflict_action: str | None = None,
    is_overwrite: bool = False,
    has_pk: bool = False,
) -> str:
    """Build ``INSERT [OVERWRITE] ... SELECT ... FROM external_files`` SQL.

    Args:
        columns: (name, normalized type name, precision, scale) tuples in the
            order they appear in the staged Arrow files.
        stages: internal stage names; every file under them is loaded.
        conflict_action: "ignore" or "update" (primary-key tables only).
        is_overwrite: emit ``INSERT OVERWRITE`` instead of ``INSERT INTO``.
        has_pk: whether the target table has a primary key.
    """
    if not stages:
        raise ValueError("At least one stage name is required")
    full_table = sql.SQL("{}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name)).as_string()

    verb = "insert overwrite" if is_overwrite else "insert into"
    col_ids = sql.SQL(", ").join(sql.Identifier(name) for name, _, _, _ in columns).as_string()

    # SELECT clause: json/jsonb columns are cast from their staged text form.
    select_items = []
    for name, tn, precision, scale in columns:
        quoted = sql.Identifier(name).as_string()
        resolved = _stage_type_name(tn, precision, scale)
        if resolved in ("json", "jsonb"):
            select_items.append(f"{quoted}::{resolved}")
        else:
            select_items.append(quoted)

    stage_paths = ",".join(f"internal_stage://{s}" for s in stages)

    as_items = []
    for name, tn, precision, scale in columns:
        quoted = sql.Identifier(name).as_string()
        resolved = _stage_type_name(tn, precision, scale)
        if resolved in ("json", "jsonb"):
            resolved = "text"
        as_items.append(f"{quoted} {resolved}")

    parts = [
        f"{verb} {full_table} (",
        col_ids,
        ") select ",
        ", ".join(select_items),
        f" from external_files(path='{stage_paths}') as (",
        ", ".join(as_items),
        ")",
    ]

    if not is_overwrite and has_pk and conflict_action is not None:
        raise NotImplementedError(
            "ON CONFLICT clause requires the primary key column list; use build_insert_select_from_stage_sql "
            "with primary_keys to resolve conflicts."
        )

    return "".join(parts)


def build_insert_select_from_stage_sql_pk(
    schema_name: str,
    table_name: str,
    columns: Sequence[tuple[str, str, int, int]],
    primary_keys: Sequence[str],
    stages: Sequence[str],
    conflict_action: str | None = None,
    is_overwrite: bool = False,
) -> str:
    """Build ``INSERT [OVERWRITE] ... SELECT ...`` with an ON CONFLICT clause.

    Same as :func:`build_insert_select_from_stage_sql` but supports resolving
    primary-key conflicts ("ignore" -> DO NOTHING, otherwise DO UPDATE).
    """
    base = build_insert_select_from_stage_sql(
        schema_name=schema_name,
        table_name=table_name,
        columns=columns,
        stages=stages,
        conflict_action=None,
        is_overwrite=is_overwrite,
    )

    if is_overwrite or not primary_keys or conflict_action is None:
        return base

    pk_ids = sql.SQL(", ").join(sql.Identifier(pk) for pk in primary_keys).as_string()
    if conflict_action == "ignore":
        return f"{base} on conflict ({pk_ids}) do nothing"

    set_items = ",".join(
        f"{sql.Identifier(name).as_string()}=excluded.{sql.Identifier(name).as_string()}" for name, _, _, _ in columns
    )
    return f"{base} on conflict ({pk_ids}) do update set {set_items}"
