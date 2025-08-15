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
        assert df == {"id": [1, 3, 5, 7, 9, 11, 13, 15, 17, 19]}

    asyncio.run(main())
