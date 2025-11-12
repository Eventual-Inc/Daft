from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest

import daft
from daft import DataType


def test_cls():
    df = daft.from_pydict({"a": ["foo", "bar", "baz"]})

    @daft.cls
    class RepeatN:
        def __init__(self, n: int):
            self.n = n

        def __call__(self, x) -> str:
            return x * self.n

        @daft.method(return_dtype=DataType.list(DataType.string()))
        def repeat_list(self, x):
            return [x] * self.n

    repeat_2 = RepeatN(2)
    result = df.select(repeat_2(df["a"]))
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"]}

    result = df.select(repeat_2.repeat_list(df["a"]))
    assert result.to_pydict() == {"a": [["foo", "foo"], ["bar", "bar"], ["baz", "baz"]]}


def test_cls_scalar_eval():
    @daft.cls
    class RepeatN:
        def __init__(self, n: int):
            self.n = n

        def __call__(self, x) -> str:
            return x * self.n

        @daft.method(return_dtype=DataType.list(DataType.string()))
        def repeat_list(self, x):
            return [x] * self.n

    repeat_2 = RepeatN(2)
    assert repeat_2("foo") == "foofoo"
    assert repeat_2.repeat_list("foo") == ["foo", "foo"]


def test_cls_method_without_decorator():
    df = daft.from_pydict({"a": [1, 2, 3]})

    @daft.cls
    class Multiplier:
        def __init__(self, factor: int):
            self.factor = factor

        def multiply(self, x: int) -> int:
            return x * self.factor

    m = Multiplier(5)
    result = df.select(m.multiply(df["a"]))
    assert result.to_pydict() == {"a": [5, 10, 15]}


def test_cls_multiple_instances():
    df = daft.from_pydict({"a": [10, 20, 30]})

    @daft.cls
    class Adder:
        def __init__(self, increment: int):
            self.increment = increment

        def add(self, x: int) -> int:
            return x + self.increment

    adder_1 = Adder(1)
    adder_10 = Adder(10)

    result = df.select(
        adder_1.add(df["a"]).alias("plus_1"),
        adder_10.add(df["a"]).alias("plus_10"),
    )
    assert result.to_pydict() == {
        "plus_1": [11, 21, 31],
        "plus_10": [20, 30, 40],
    }


@pytest.mark.parametrize("concurrency", [None, 1, 2])
def test_cls_async_method(concurrency):
    df = daft.from_pydict({"a": [1, 2, 3]})

    @daft.cls(max_concurrency=concurrency)
    class AsyncProcessor:
        def __init__(self, delay: float):
            self.delay = delay

        async def process(self, x: int) -> int:
            await asyncio.sleep(self.delay)
            return x * 2

    processor = AsyncProcessor(0.01)
    result = df.select(processor.process(df["a"]))
    assert sorted(result.to_pydict()["a"]) == [2, 4, 6]


def test_cls_generator_method():
    df = daft.from_pydict({"id": [0, 1, 2], "n": [0, 2, 3]})

    @daft.cls
    class RepeatGenerator:
        def __init__(self, value: str):
            self.value = value

        def generate(self, n: int) -> Iterator[str]:
            for _ in range(n):
                yield self.value

    gen = RepeatGenerator("x")
    result = df.select("id", gen.generate(df["n"])).collect()
    assert result.to_pydict() == {
        "id": [0, 1, 1, 2, 2, 2],
        "n": [None, "x", "x", "x", "x", "x"],
    }


def test_cls_unnest_struct():
    df = daft.from_pydict({"a": [1, 2, 3]})

    @daft.cls
    class Processor:
        def __init__(self, multiplier: int):
            self.multiplier = multiplier

        @daft.method(
            return_dtype=DataType.struct({"doubled": DataType.int64(), "name": DataType.string()}),
            unnest=True,
        )
        def process(self, x: int):
            return {"doubled": x * self.multiplier, "name": f"value_{x}"}

    processor = Processor(2)
    result = df.select(processor.process(df["a"]))
    assert result.to_pydict() == {
        "doubled": [2, 4, 6],
        "name": ["value_1", "value_2", "value_3"],
    }


def test_cls_multiple_methods():
    df = daft.from_pydict({"text": ["hello", "world", "daft"]})

    @daft.cls
    class TextProcessor:
        def __init__(self, prefix: str):
            self.prefix = prefix

        def add_prefix(self, text: str) -> str:
            return f"{self.prefix}{text}"

        @daft.method(return_dtype=DataType.list(DataType.string()))
        def split_chars(self, text: str):
            return list(text)

        def length(self, text: str) -> int:
            return len(text)

    processor = TextProcessor("pre_")
    result = df.select(
        processor.add_prefix(df["text"]).alias("prefixed"),
        processor.split_chars(df["text"]).alias("chars"),
        processor.length(df["text"]).alias("len"),
    )
    expected = {
        "prefixed": ["pre_hello", "pre_world", "pre_daft"],
        "chars": [list("hello"), list("world"), list("daft")],
        "len": [5, 5, 4],
    }
    assert result.to_pydict() == expected


def test_cls_with_complex_init():
    df = daft.from_pydict({"a": [1, 2, 3]})

    @daft.cls
    class Calculator:
        def __init__(self, multiplier: int, offset: int, name: str):
            self.multiplier = multiplier
            self.offset = offset
            self.name = name

        def compute(self, x: int) -> int:
            return (x * self.multiplier) + self.offset

        def get_name(self, x: int) -> str:
            return f"{self.name}_{x}"

    calc = Calculator(multiplier=10, offset=5, name="calc")
    result = df.select(
        calc.compute(df["a"]).alias("result"),
        calc.get_name(df["a"]).alias("name"),
    )
    assert result.to_pydict() == {
        "result": [15, 25, 35],
        "name": ["calc_1", "calc_2", "calc_3"],
    }


def test_cls_with_list_operations():
    df = daft.from_pydict({"lists": [[1, 2, 3], [4, 5], [6, 7, 8, 9]]})

    @daft.cls
    class ListProcessor:
        def __init__(self, threshold: int):
            self.threshold = threshold

        @daft.method(return_dtype=DataType.list(DataType.int64()))
        def filter_above(self, lst):
            return [x for x in lst if x > self.threshold]

        def count_above(self, lst) -> int:
            return sum(1 for x in lst if x > self.threshold)

    processor = ListProcessor(5)
    result = df.select(
        processor.filter_above(df["lists"]).alias("filtered"),
        processor.count_above(df["lists"]).alias("count"),
    )
    assert result.to_pydict() == {
        "filtered": [[], [], [6, 7, 8, 9]],
        "count": [0, 0, 4],
    }


def test_cls_batch_method():
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})

    @daft.cls
    class BatchAdder:
        def __init__(self, offset: int):
            self.offset = offset

        @daft.method.batch(return_dtype=DataType.int64())
        def add(self, a: daft.Series, b: daft.Series) -> daft.Series:
            import pyarrow.compute as pc

            a_arrow = a.to_arrow()
            b_arrow = b.to_arrow()
            result = pc.add(a_arrow, b_arrow)
            result = pc.add(result, self.offset)
            return daft.Series.from_arrow(result)

    adder = BatchAdder(10)
    result = df.select(adder.add(df["a"], df["b"]))
    assert result.to_pydict() == {"a": [15, 17, 19]}


def test_cls_batch_method_scalar_eval():
    @daft.cls
    class BatchMultiplier:
        def __init__(self, factor: int):
            self.factor = factor

        @daft.method.batch(return_dtype=DataType.int64())
        def multiply(self, a: daft.Series) -> daft.Series:
            import pyarrow.compute as pc

            a_arrow = a.to_arrow()
            result = pc.multiply(a_arrow, self.factor)
            return daft.Series.from_arrow(result)

    multiplier = BatchMultiplier(5)
    # When called with a scalar, should execute eagerly
    a = daft.Series.from_pylist([1, 2, 3])
    assert multiplier.multiply(a).to_pylist() == [5, 10, 15]


@pytest.mark.parametrize("concurrency", [None, 1, 2])
def test_cls_async_batch_method(concurrency):
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})

    @daft.cls(max_concurrency=concurrency)
    class AsyncBatchProcessor:
        def __init__(self, delay: float):
            self.delay = delay

        @daft.method.batch(return_dtype=DataType.int64())
        async def process(self, a: daft.Series) -> daft.Series:
            await asyncio.sleep(self.delay)
            return a

    processor = AsyncBatchProcessor(delay=0.01)
    result = df.select(processor.process(df["a"]))
    assert sorted(result.to_pydict()["a"]) == [1, 2, 3]


def test_cls_max_concurrency_zero():
    with pytest.raises(ValueError, match="max_concurrency for udf must be non-zero"):

        @daft.cls(max_concurrency=0)
        class MaxConcurrencyZero:
            def __init__(self):
                pass

            def __call__(self, x: int) -> int:
                return x

        df = daft.from_pydict({"a": [1, 2, 3]})
        result = df.select(MaxConcurrencyZero()(df["a"])).to_pydict()
        assert result == {"a": [1, 2, 3]}
