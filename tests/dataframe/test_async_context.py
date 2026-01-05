from __future__ import annotations


def test_dataframe_running_in_async_context():
    import asyncio

    import daft

    async def main():
        @daft.func
        async def add_one(x: int) -> int:
            await asyncio.sleep(0.1)
            return x + 1

        df = (
            daft.range(100, partitions=10)
            .where(daft.col("id") % 2 == 0)
            .with_column("id", add_one(daft.col("id")))
            .limit(10)
            .to_pydict()
        )
        # Sort to handle non-deterministic ordering from limit()
        result = sorted(df["id"])
        # Check that we get 10 odd numbers in the expected range [1, 99]
        assert len(result) == 10
        assert all(x % 2 == 1 for x in result)
        assert all(1 <= x <= 99 for x in result)

    asyncio.run(main())
