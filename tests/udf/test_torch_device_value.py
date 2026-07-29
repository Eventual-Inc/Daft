from __future__ import annotations

import gc
import json
import weakref
from dataclasses import asdict, dataclass, field
from typing import Any

import pytest

import daft
from daft import DataType, Series, col
from tests.conftest import get_tests_daft_runner_name


@dataclass
class _DataMovement:
    phase: str
    source: str
    destination: str
    byte_count: int
    reason: str
    measurement: str


@dataclass
class _CudaBatchOwner:
    tensor: Any
    created_pointers: tuple[int, ...]
    created_owner_id: int = 0
    observed_pointers: tuple[int, ...] = ()
    observed_devices: tuple[str, ...] = ()
    observed_owner_ids: tuple[int, ...] = ()
    sync_guard_error: str | None = None
    ingress_arrow_ptr: int = 0
    ingress_torch_ptr: int = 0
    mask_torch_ptr: int = 0
    mask_arrow_ptr: int = 0
    movements: list[_DataMovement] = field(default_factory=list)


@dataclass
class _CudaRowValue:
    owner: _CudaBatchOwner
    index: int


def _declared_device_transfer(
    torch: Any,
    tensor: Any,
    destination: str,
    *,
    phase: str,
    reason: str,
    movements: list[_DataMovement],
) -> Any:
    previous_mode = torch.cuda.get_sync_debug_mode()
    assert previous_mode == 2, "all CUDA work outside declared transfers must run with sync debugging set to error"

    torch.cuda.set_sync_debug_mode("default")
    try:
        transferred = tensor.to(destination)
    finally:
        torch.cuda.set_sync_debug_mode(previous_mode)

    movements.append(
        _DataMovement(
            phase=phase,
            source=str(tensor.device),
            destination=str(transferred.device),
            byte_count=tensor.numel() * tensor.element_size(),
            reason=reason,
            measurement="physical_buffer_bytes",
        )
    )
    return transferred


_DECLARED_SERIES_ITERATIONS: set[int] = set()
_DECLARED_TENSOR_LISTS: set[int] = set()


def _declared_series_list(
    values: Series,
    *,
    phase: str,
    source: str,
    destination: str,
    byte_count: int,
    reason: str,
    measurement: str,
    movements: list[_DataMovement],
) -> list[Any]:
    _DECLARED_SERIES_ITERATIONS.add(id(values))
    try:
        result = list(values)
    finally:
        _DECLARED_SERIES_ITERATIONS.remove(id(values))
    movements.append(
        _DataMovement(
            phase=phase,
            source=source,
            destination=destination,
            byte_count=byte_count,
            reason=reason,
            measurement=measurement,
        )
    )
    return result


def _declared_tensor_list(
    tensor: Any,
    *,
    phase: str,
    source: str,
    destination: str,
    byte_count: int,
    reason: str,
    movements: list[_DataMovement],
) -> list[Any]:
    _DECLARED_TENSOR_LISTS.add(id(tensor))
    try:
        result = tensor.tolist()
    finally:
        _DECLARED_TENSOR_LISTS.remove(id(tensor))
    movements.append(
        _DataMovement(
            phase=phase,
            source=source,
            destination=destination,
            byte_count=byte_count,
            reason=reason,
            measurement="logical_payload_bytes",
        )
    )
    return result


def _reject_materialization(route: str) -> Any:
    def reject(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(f"undeclared materialization through {route}")

    return reject


def _install_deep_path_guards(monkeypatch: pytest.MonkeyPatch, torch: Any) -> tuple[str, ...]:
    from daft.daft import PySeries
    from daft.dataframe import DataFrame
    from daft.recordbatch import MicroPartition, RecordBatch

    guarded_routes: list[tuple[type[Any], str]] = [
        (torch.Tensor, "cpu"),
        (torch.Tensor, "numpy"),
        (Series, "to_pylist"),
        (PySeries, "to_pylist"),
        (DataFrame, "to_pylist"),
        (DataFrame, "to_pydict"),
        (MicroPartition, "to_pylist"),
        (MicroPartition, "to_pydict"),
        (RecordBatch, "to_pylist"),
        (RecordBatch, "to_pydict"),
    ]
    original_series_iter = Series.__iter__
    original_tensor_tolist = torch.Tensor.tolist

    def guarded_series_iter(values: Series) -> Any:
        if id(values) not in _DECLARED_SERIES_ITERATIONS:
            raise AssertionError("undeclared materialization through daft.Series.__iter__")
        return original_series_iter(values)

    def guarded_tensor_tolist(tensor: Any) -> Any:
        if id(tensor) not in _DECLARED_TENSOR_LISTS:
            raise AssertionError("undeclared materialization through torch.Tensor.tolist")
        return original_tensor_tolist(tensor)

    monkeypatch.setattr(Series, "__iter__", guarded_series_iter)
    monkeypatch.setattr(torch.Tensor, "tolist", guarded_tensor_tolist)

    try:
        import cupy
    except ImportError:
        pass
    else:
        guarded_routes.append((cupy.ndarray, "get"))

    installed: list[str] = []
    for owner, method_name in guarded_routes:
        route = f"{owner.__module__}.{owner.__name__}.{method_name}"
        monkeypatch.setattr(owner, method_name, _reject_materialization(route))
        installed.append(route)
    installed.extend(("daft.Series.__iter__", "torch.Tensor.tolist"))
    return tuple(installed)


def _run_device_value_spike(
    monkeypatch: pytest.MonkeyPatch, torch: Any
) -> tuple[dict[str, Any], weakref.ReferenceType[Any], weakref.ReferenceType[Any]]:
    @daft.func.batch(return_dtype=DataType.python())
    def place_on_cuda(values: Series) -> Series:
        movements: list[_DataMovement] = []
        arrow_values = values.to_arrow()
        arrow_buffer = arrow_values.buffers()[1]
        assert arrow_buffer is not None
        python_values = _declared_series_list(
            values,
            phase="host_ingress_list",
            source="daft_arrow_values",
            destination="python_list",
            byte_count=len(values) * 8,
            reason="extract numeric values from the Daft batch",
            measurement="logical_payload_bytes",
            movements=movements,
        )
        host_values = torch.tensor(python_values, dtype=torch.float32)
        movements.append(
            _DataMovement(
                phase="host_ingress_tensor",
                source="python_list",
                destination="torch_cpu",
                byte_count=host_values.numel() * host_values.element_size(),
                reason="construct the dense CPU tensor used for CUDA ingress",
                measurement="physical_buffer_bytes",
            )
        )
        cuda_values = _declared_device_transfer(
            torch,
            host_values,
            "cuda",
            phase="device_ingress",
            reason="create the CUDA values requested by this feasibility spike",
            movements=movements,
        )
        owner = _CudaBatchOwner(
            tensor=cuda_values,
            created_pointers=tuple(cuda_values[index].data_ptr() for index in range(len(cuda_values))),
            ingress_arrow_ptr=arrow_buffer.address + arrow_values.offset * 8,
            ingress_torch_ptr=host_values.data_ptr(),
            movements=movements,
        )
        owner.created_owner_id = id(owner)
        wrapper_values = [_CudaRowValue(owner=owner, index=index) for index in range(len(cuda_values))]
        result = Series.from_pylist(
            wrapper_values,
            dtype=DataType.python(),
        )
        movements.append(
            _DataMovement(
                phase="device_wrapper_series",
                source="python_cuda_row_wrappers",
                destination="daft_python_series",
                byte_count=0,
                reason="store references to CUDA rows without moving tensor payload",
                measurement="zero_copy_tensor_payload",
            )
        )
        return result

    @daft.func.batch(return_dtype=DataType.bool())
    def create_host_filter_mask(device_values: Series) -> Series:
        wrapper_movements: list[_DataMovement] = []
        rows = _declared_series_list(
            device_values,
            phase="device_wrapper_list",
            source="daft_python_series",
            destination="python_cuda_row_wrappers",
            byte_count=0,
            reason="inspect CUDA row references in the second UDF",
            measurement="zero_copy_tensor_payload",
            movements=wrapper_movements,
        )
        assert rows

        owner = rows[0].owner
        owner.movements.extend(wrapper_movements)
        assert all(row.owner is owner for row in rows)
        owner.observed_owner_ids = tuple(id(row.owner) for row in rows)
        owner.observed_pointers = tuple(row.owner.tensor[row.index].data_ptr() for row in rows)
        owner.observed_devices = tuple(str(row.owner.tensor[row.index].device) for row in rows)

        cuda_mask = owner.tensor > 2
        try:
            cuda_mask.sum().item()
        except RuntimeError as error:
            owner.sync_guard_error = str(error)
        else:
            raise AssertionError("sync debugging did not reject an undeclared CUDA synchronization")

        host_mask = _declared_device_transfer(
            torch,
            cuda_mask,
            "cpu",
            phase="selection_boundary",
            reason="Daft's native filter requires a host Boolean Series",
            movements=owner.movements,
        )
        owner.mask_torch_ptr = host_mask.data_ptr()
        mask_values = _declared_tensor_list(
            host_mask,
            phase="host_mask_list",
            source="torch_cpu",
            destination="python_list",
            byte_count=host_mask.numel() * host_mask.element_size(),
            reason="convert the host mask into values accepted by Daft's Boolean constructor",
            movements=owner.movements,
        )
        result = Series.from_pylist(mask_values, dtype=DataType.bool())
        result_arrow = result.to_arrow()
        result_buffer = result_arrow.buffers()[1]
        assert result_buffer is not None
        owner.mask_arrow_ptr = result_buffer.address
        owner.movements.append(
            _DataMovement(
                phase="host_mask_series",
                source="python_list",
                destination="daft_boolean_buffer",
                byte_count=result_buffer.size,
                reason="construct the host Boolean Series consumed by Daft's native filter",
                measurement="physical_buffer_bytes",
            )
        )
        return result

    source = daft.from_pydict({"row_id": [101, 102, 103, 104], "value": [1, 2, 3, 4]})
    query = (
        source.with_column("device_value", place_on_cuda(col("value")))
        .with_column("keep", create_host_filter_mask(col("device_value")))
        .filter(col("keep"))
        .select("row_id", "value", "device_value")
    )

    with monkeypatch.context() as deep_path_guards:
        guarded_routes = _install_deep_path_guards(deep_path_guards, torch)
        collected = query.collect()

    result = collected.to_pydict()
    assert result["row_id"] == [103, 104]
    assert result["value"] == [3, 4]

    retained_rows = result["device_value"]
    assert [row.index for row in retained_rows] == [2, 3]
    owner = retained_rows[0].owner
    assert all(row.owner is owner for row in retained_rows)
    assert owner.created_pointers == owner.observed_pointers
    assert owner.observed_owner_ids == (owner.created_owner_id,) * 4
    assert owner.observed_devices == ("cuda:0",) * 4
    assert owner.sync_guard_error is not None
    assert "synchronizing CUDA operation" in owner.sync_guard_error

    assert owner.ingress_arrow_ptr != owner.ingress_torch_ptr
    assert owner.mask_torch_ptr != owner.mask_arrow_ptr
    movements = [asdict(movement) for movement in owner.movements]
    assert movements == [
        {
            "phase": "host_ingress_list",
            "source": "daft_arrow_values",
            "destination": "python_list",
            "byte_count": 32,
            "reason": "extract numeric values from the Daft batch",
            "measurement": "logical_payload_bytes",
        },
        {
            "phase": "host_ingress_tensor",
            "source": "python_list",
            "destination": "torch_cpu",
            "byte_count": 16,
            "reason": "construct the dense CPU tensor used for CUDA ingress",
            "measurement": "physical_buffer_bytes",
        },
        {
            "phase": "device_ingress",
            "source": "cpu",
            "destination": "cuda:0",
            "byte_count": 16,
            "reason": "create the CUDA values requested by this feasibility spike",
            "measurement": "physical_buffer_bytes",
        },
        {
            "phase": "device_wrapper_series",
            "source": "python_cuda_row_wrappers",
            "destination": "daft_python_series",
            "byte_count": 0,
            "reason": "store references to CUDA rows without moving tensor payload",
            "measurement": "zero_copy_tensor_payload",
        },
        {
            "phase": "device_wrapper_list",
            "source": "daft_python_series",
            "destination": "python_cuda_row_wrappers",
            "byte_count": 0,
            "reason": "inspect CUDA row references in the second UDF",
            "measurement": "zero_copy_tensor_payload",
        },
        {
            "phase": "selection_boundary",
            "source": "cuda:0",
            "destination": "cpu",
            "byte_count": 4,
            "reason": "Daft's native filter requires a host Boolean Series",
            "measurement": "physical_buffer_bytes",
        },
        {
            "phase": "host_mask_list",
            "source": "torch_cpu",
            "destination": "python_list",
            "byte_count": 4,
            "reason": "convert the host mask into values accepted by Daft's Boolean constructor",
            "measurement": "logical_payload_bytes",
        },
        {
            "phase": "host_mask_series",
            "source": "python_list",
            "destination": "daft_boolean_buffer",
            "byte_count": 1,
            "reason": "construct the host Boolean Series consumed by Daft's native filter",
            "measurement": "physical_buffer_bytes",
        },
    ]
    assert not any(movement["phase"] == "between_udfs" for movement in movements)

    summary = {
        "created_pointers": owner.created_pointers,
        "observed_pointers": owner.observed_pointers,
        "device": owner.observed_devices[0],
        "selected_row_ids": result["row_id"],
        "ingress_arrow_ptr": owner.ingress_arrow_ptr,
        "ingress_torch_ptr": owner.ingress_torch_ptr,
        "mask_torch_ptr": owner.mask_torch_ptr,
        "mask_arrow_ptr": owner.mask_arrow_ptr,
        "movements": movements,
        "guarded_routes": guarded_routes,
        "sync_guard_error": owner.sync_guard_error,
    }
    owner_reference = weakref.ref(owner)
    tensor_reference = weakref.ref(owner.tensor)
    return summary, owner_reference, tensor_reference


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires the native runner")
def test_cuda_python_value_preserves_pointer_until_native_filter_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    torch = pytest.importorskip("torch")
    if not torch.cuda.is_available():
        pytest.skip("requires a CUDA device")

    previous_sync_debug_mode = torch.cuda.get_sync_debug_mode()
    try:
        torch.cuda.set_sync_debug_mode("error")
        summary, owner_reference, tensor_reference = _run_device_value_spike(monkeypatch, torch)
    finally:
        torch.cuda.set_sync_debug_mode(previous_sync_debug_mode)

    gc.collect()
    torch.cuda.empty_cache()
    assert owner_reference() is None
    assert tensor_reference() is None
    assert torch.cuda.get_sync_debug_mode() == previous_sync_debug_mode

    print(f"DEVICE_SPIKE_RESULT={json.dumps(summary, sort_keys=True)}")
