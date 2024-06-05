import timeit
import json

import numpy as np
import pyarrow as pa
from pyarrow import compute


def measured_us(f, number=512):
    seconds = timeit.Timer(f).timeit(number=number) / number
    microseconds = seconds * 1000 * 1000
    return microseconds


def get_f32(size, null_density):
    if null_density == 0:
        validity = None
    else:
        validity = np.random.random(size=size) < null_density
    values = np.random.random(size=size)
    return values, validity


# high null density is high number of ones/True, as pyarrow considers 1 in the mask to be a null.
assert sum([x for x in get_f32(1000, 0.9)[1] if x]) > 750


def bench_add_f32_pyarrow(log2_size, null_density):
    size = 2**log2_size

    values, validity = get_f32(size, null_density)
    array1 = pa.array(values, pa.float32(), mask=validity)

    values, validity = get_f32(size, null_density)
    array2 = pa.array(values, pa.float32(), mask=validity)

    def f():
        pa.compute.add(array1, array2)

    microseconds = measured_us(f)
    print(f"add pyarrow f32 2^{log2_size}     time: {microseconds:.2f} us")
    return {
        "operation": "add",
        "log2_size": log2_size,
        "type": "float32",
        "time (us)": microseconds,
        "null_density": null_density,
        "backend": "pyarrow",
    }


def bench_add_f32_numpy(log2_size):
    size = 2**log2_size

    array1, _ = get_f32(size, 0)
    array2, _ = get_f32(size, 0)

    def f():
        np.add(array1, array2)

    microseconds = measured_us(f)
    print(f"add numpy f32 2^{log2_size}     time: {microseconds:.2f} us")
    return {
        "operation": "add",
        "log2_size": log2_size,
        "type": "float32",
        "time (us)": microseconds,
        "null_density": 0,
        "backend": "numpy",
    }


def _bench_unary_f32_pyarrow(log2_size, null_density, name, op):
    size = 2**log2_size

    values, validity = get_f32(size, null_density)
    array = pa.array(values, pa.float32(), mask=validity)

    def f():
        op(array)

    microseconds = measured_us(f)
    print(f"{name} f32 2^{log2_size}     time: {microseconds:.2f} us")
    return {
        "operation": name,
        "log2_size": log2_size,
        "type": "float32",
        "time (us)": microseconds,
        "null_density": null_density,
        "backend": "pyarrow",
    }


def bench_sum_f32_pyarrow(log2_size, null_density):
    return _bench_unary_f32_pyarrow(log2_size, null_density, "sum", pa.compute.sum)


def bench_min_f32_pyarrow(log2_size, null_density):
    return _bench_unary_f32_pyarrow(log2_size, null_density, "min", pa.compute.min_max)


def _bench_unary_f32_numpy(log2_size, name, op):
    size = 2**log2_size

    values, _ = get_f32(size, 0)

    def f():
        op(values)

    microseconds = measured_us(f)
    print(f"{name} f32 2^{log2_size}     time: {microseconds:.2f} us")
    return {
        "operation": name,
        "log2_size": log2_size,
        "type": "float32",
        "time (us)": microseconds,
        "null_density": 0,
        "backend": "numpy",
    }


def bench_sum_f32_numpy(log2_size):
    return _bench_unary_f32_numpy(log2_size, "sum", np.sum)


def bench_min_f32_numpy(log2_size):
    return _bench_unary_f32_numpy(log2_size, "min", np.min)


def bench_sort_f32_pyarrow(log2_size, null_density):
    size = 2**log2_size

    values, validity = get_f32(size, null_density)
    array = pa.array(values, pa.float32(), mask=validity)

    def f():
        indices = pa.compute.sort_indices(array)
        pa.compute.take(array, indices)

    microseconds = measured_us(f)
    print(f"sort f32 2^{log2_size}     time: {microseconds:.2f} us")
    return {
        "operation": "sort",
        "log2_size": log2_size,
        "type": "float32",
        "time (us)": microseconds,
        "null_density": null_density,
        "backend": "pyarrow",
    }


def bench_sort_f32_numpy(log2_size):
    null_density = 0
    size = 2**log2_size

    array, _ = get_f32(size, null_density)

    def f():
        np.sort(array)

    microseconds = measured_us(f)
    print(f"sort f32 2^{log2_size}     time: {microseconds:.2f} us")
    return {
        "operation": "sort",
        "log2_size": log2_size,
        "type": "float32",
        "time (us)": microseconds,
        "null_density": null_density,
        "backend": "numpy",
    }


def bench_filter_f32_pyarrow(log2_size, null_density):
    size = 2**log2_size

    values, validity = get_f32(size, null_density)
    _, mask = get_f32(size, 0.9)
    count_valids = sum([x for x in mask if x])
    array = pa.array(values, pa.float32(), mask=validity)
    mask_ = pa.array(mask, pa.bool_())

    count_valids = sum([bool(x) for x in mask if x])

    def f():
        pa.compute.filter(array, mask_)

    microseconds = measured_us(f, 1)
    print(f"filter f32 2^{log2_size}     time: {microseconds:.2f} us")
    return {
        "operation": "filter",
        "log2_size": log2_size,
        "type": "float32",
        "time (us)": microseconds,
        "null_density": null_density,
        "backend": "pyarrow",
    }


def bench_filter_f32_numpy(log2_size):
    null_density = 0
    size = 2**log2_size

    array, _ = get_f32(size, null_density)
    _, mask = get_f32(size, 0.1)

    def f():
        array[mask]

    microseconds = measured_us(f)
    print(f"filter f32 2^{log2_size}     time: {microseconds:.2f} us")
    return {
        "operation": "filter",
        "log2_size": log2_size,
        "type": "float32",
        "time (us)": microseconds,
        "null_density": null_density,
        "backend": "numpy",
    }


def measure():
    results = []
    for size in range(10, 21, 2):
        results.append(bench_add_f32_numpy(size))
        results.append(bench_add_f32_pyarrow(size, 0))
        results.append(bench_add_f32_pyarrow(size, 0.1))
        results.append(bench_sum_f32_numpy(size))
        results.append(bench_sum_f32_pyarrow(size, 0))
        results.append(bench_sum_f32_pyarrow(size, 0.1))
        results.append(bench_min_f32_numpy(size))
        results.append(bench_min_f32_pyarrow(size, 0))
        results.append(bench_min_f32_pyarrow(size, 0.1))
        results.append(bench_sort_f32_numpy(size))
        results.append(bench_sort_f32_pyarrow(size, 0))
        results.append(bench_sort_f32_pyarrow(size, 0.1))
        results.append(bench_filter_f32_pyarrow(size, 0))
        results.append(bench_filter_f32_pyarrow(size, 0.1))
        results.append(bench_filter_f32_numpy(size))

    with open("result.json", "w") as f:
        json.dump(results, f, indent=4)


def report(operation, backend, null_density):
    print(operation, null_density)
    print(backend)
    with open("result.json", "r") as f:
        results = json.load(f)
    for result in results:
        if (
            result["operation"] == operation
            and result["backend"] == backend
            and result["null_density"] == null_density
        ):
            print(result["time (us)"])


measure()
op = "min"
report(op, "numpy", 0)
report(op, "pyarrow", 0.1)
report(op, "pyarrow", 0)
