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
        # Each output is `add_one(even input)`, so every value is odd and in
        # (0, 100]. Distributed Limit may pull from any partition that
        # finishes first, so don't assert the specific 10 values.
        result = df["id"]
        assert len(result) == 10
        assert len(set(result)) == 10
        assert all(x % 2 == 1 and 1 <= x <= 99 for x in result)

    asyncio.run(main())
