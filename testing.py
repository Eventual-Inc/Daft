import benchmarking.tpch.answers as daft_queries
import daft


def daft_run(query, explain=False):
    def _get_df(tbl_name: str):
        fp = f"s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/10_0/32/parquet/{tbl_name}/"
        df = daft.read_parquet(fp)
        return df

    d = getattr(daft_queries, f"q{query}")(_get_df)
    if explain:
        d.explain(True)
    return d.to_arrow()


def basic_test():
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
    df = df.select("a").where(daft.col("a") > 2)
    # df.explain(True)

    print(df.collect())


def run_questions():
    # get questions from args
    import sys

    questions = [int(q) for q in sys.argv[1:]]
    if not questions:
        questions = list(range(1, 23))
    for q in questions:
        daft_run(q, explain=False)


if __name__ == "__main__":
    basic_test()
    # run_questions()
