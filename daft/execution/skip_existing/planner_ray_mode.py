from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from daft.daft import FileFormat

from .key_filter_ray_mode import build_key_filter_predicate
from .key_spec import KeySpec
from .resources_ray_mode import build_key_filter_resources, cleanup_key_filter_resources

if TYPE_CHECKING:
    from collections.abc import Callable

    from daft import DataFrame
    from daft.logical.builder import LogicalPlanBuilder


def maybe_apply_skip_existing(builder: LogicalPlanBuilder) -> tuple[LogicalPlanBuilder, Any | None]:
    specs = builder._builder.get_skip_existing_specs()
    if not specs:
        return builder, None

    cleanup_items: list[tuple[list[Any], Any | None]] = []
    predicates: list[Any | None] = []

    def cleanup() -> None:
        for actor_handles, placement_group in cleanup_items:
            cleanup_key_filter_resources(actor_handles, placement_group)

    try:
        for spec in specs:
            existing_path = spec.existing_path
            file_format = spec.file_format
            key_column = spec.key_column
            io_config = spec.io_config
            num_workers = spec.num_workers
            cpus_per_worker = spec.cpus_per_worker
            read_kwargs = spec.read_kwargs
            keys_load_batch_size = spec.keys_load_batch_size
            max_concurrency_per_worker = spec.max_concurrency_per_worker
            strict_path_check = spec.strict_path_check

            if num_workers is None:
                raise RuntimeError("[skip_existing] num_workers must be provided")
            if cpus_per_worker is None:
                raise RuntimeError("[skip_existing] cpus_per_worker must be provided")
            if keys_load_batch_size is None:
                raise RuntimeError("[skip_existing] keys_load_batch_size must be provided")
            if max_concurrency_per_worker is None:
                raise RuntimeError("[skip_existing] max_concurrency_per_worker must be provided")

            key_spec = KeySpec(key_column)
            key_expr = key_spec.to_expr()

            read_fn: Callable[..., DataFrame]
            if file_format == FileFormat.Parquet:
                from daft.io._parquet import read_parquet

                read_fn = cast("Callable[..., DataFrame]", read_parquet)
            elif file_format == FileFormat.Csv:
                from daft.io._csv import read_csv

                read_fn = cast("Callable[..., DataFrame]", read_csv)
            elif file_format == FileFormat.Json:
                from daft.io._json import read_json

                read_fn = cast("Callable[..., DataFrame]", read_json)
            else:
                raise ValueError(f"[skip_existing] Unsupported file format: {file_format}")

            actor_handles, placement_group = build_key_filter_resources(
                existing_path=cast("Any", existing_path),
                io_config=io_config,
                key_spec=key_spec,
                num_workers=num_workers,
                cpus_per_worker=cpus_per_worker,
                read_fn=read_fn,
                read_kwargs=read_kwargs,
                keys_load_batch_size=keys_load_batch_size,
                max_concurrency_per_worker=max_concurrency_per_worker,
                strict_path_check=strict_path_check,
            )

            if not actor_handles:
                predicates.append(None)
                continue

            key_filter_expr = build_key_filter_predicate(
                num_workers,
                actor_handles,
                key_spec,
            )(key_expr)
            cleanup_items.append((actor_handles, placement_group))
            predicates.append(key_filter_expr)

        new_inner_builder = builder._builder.apply_skip_existing_predicates(
            [p._expr if p is not None else None for p in predicates]
        )
        from daft.logical.builder import LogicalPlanBuilder

        new_builder = LogicalPlanBuilder(new_inner_builder)
    except Exception:
        cleanup()
        raise

    return new_builder, cleanup
