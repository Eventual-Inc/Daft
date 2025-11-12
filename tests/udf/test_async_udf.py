import asyncio
import daft


# Minimal async in UDF: return list and use asyncio.run in sync context
@daft.udf(return_dtype=daft.DataType.int64())
class AsyncInUDFRepro:
    def __init__(self, sleep: float = 0.0):
        self.sleep = float(sleep)

    def __call__(self, x):
        # Safely run async work in a synchronous call
        async def work(v):
            await asyncio.sleep(self.sleep)
            return v

        return asyncio.get_event_loop().run_until_complete(work(x))


def test_async_udf_rowwise_list():
    df = daft.from_pydict({"audio_size": [1, 3]})
    df = df.with_column(
        "repro_out",
        AsyncInUDFRepro.with_init_args(sleep=1)(df["audio_size"]),
    )
    assert df.to_pydict()["repro_out"] == [1, 3]
