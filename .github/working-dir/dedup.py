import daft
from daft import DataFrame, DataType, Expression, Series, col


# deduplication without connected components
def dedupe(
    df: DataFrame, id_col: Expression, text_col: Expression, num_hashes: int, ngram_size: int, rows_per_band: int
) -> DataFrame:
    df = df.select(id_col.alias("_ids"), text_col.minhash(num_hashes, ngram_size).alias("_minhash"))
    df = df.select(col("_ids"), col("_minhash").list.chunk(rows_per_band)).explode("_minhash")
    df = df.groupby("_minhash").agg_list("_ids").where(col("_ids").list.lengths() > 1)
    df = df.select(col("_ids").list.sort())

    df = df.select(col("_ids").list.slice(1).alias("u"), col("_ids").list.get(0).alias("v"))
    return df.explode("u").distinct()


def dedupe_cc(
    df: DataFrame, id_col: Expression, text_col: Expression, num_hashes: int, ngram_size: int, rows_per_band: int
) -> DataFrame:
    df = dedupe(df, id_col, text_col, num_hashes, ngram_size, rows_per_band)
    return components(df)


def e(u, v):
    return {"u": u, "v": v}


edge_dtype = DataType.struct({"u": DataType.string(), "v": DataType.string()})
edge_list_dtype = DataType.list(edge_dtype)


@daft.udf(return_dtype=edge_list_dtype)
def large_star_map(ulist: Series, vlist: Series):
    return [[e(u, v), e(v, u)] for u, v in zip(ulist.to_pylist(), vlist.to_pylist())]


@daft.udf(return_dtype=edge_list_dtype)
def large_star_reduce(ulist: Series, vlist: Series):
    out = []
    for u, vs in zip(ulist.to_pylist(), vlist.to_pylist()):
        m = min(u, *vs)
        out.append([e(v, m) for v in vs if v > u])
    return out


@daft.udf(return_dtype=edge_dtype)
def small_star_map(ulist: Series, vlist: Series):
    return [e(u, v) if u > v else e(v, u) for u, v in zip(ulist.to_pylist(), vlist.to_pylist())]


@daft.udf(return_dtype=edge_list_dtype)
def small_star_reduce(ulist: Series, vlist: Series):
    out = []
    for u, vs in zip(ulist.to_pylist(), vlist.to_pylist()):
        m = min(u, *vs)
        cur = [e(v, m) for v in vs if v <= u and v != m]
        if u != m:
            cur.append(e(u, m))
        out.append(cur)
    return out


# assumes columns are (u, v)
def components(df: DataFrame) -> DataFrame:
    b = df.select(daft.to_struct("u", "v").alias("e")).collect()
    while True:
        a = (
            b.select(large_star_map(col("e.u"), col("e.v")).alias("e"))
            .explode("e")
            .select("e.*")
            .groupby("u")
            .agg_list("v")
            .select(large_star_reduce(col("u"), col("v")).alias("e"))
            .explode("e")
            .where(~col("e").is_null())
            .distinct()
            .collect()
        )
        b = (
            a.select(small_star_map(col("e.u"), col("e.v")).alias("e"))
            .select("e.*")
            .groupby("u")
            .agg_list("v")
            .select(small_star_reduce(col("u"), col("v")).alias("e"))
            .explode("e")
            .where(~col("e").is_null())
            .distinct()
            .collect()
        )
        # check convergence
        a_hash = a.select(col("e").hash().alias("hash")).sum("hash").to_pydict()["hash"][0]
        b_hash = b.select(col("e").hash().alias("hash")).sum("hash").to_pydict()["hash"][0]
        if a_hash == b_hash:
            return b.select("e.*")


if __name__ == "__main__":
    daft.set_execution_config(enable_ray_tracing=True)

    df = daft.read_parquet("s3://eventual-dev-benchmarking-fixtures/redpajama-parquet/v1.0.0/sample-0.01")
    df = dedupe(
        df,
        col("doc_id"),
        col("raw_content"),
        128,
        13,
        25,
    ).select(
        col("u").alias("doc_id"),
        col("v").alias("original_doc_id"),
    )

    print(daft.context.get_context())
    print(df.explain(True))
    df.collect()
    print(df)
