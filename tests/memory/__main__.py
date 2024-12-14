import argparse

import daft


@daft.udf(return_dtype=str)
def to_pylist_identity(s):
    data = s.to_pylist()
    return data


@daft.udf(return_dtype=str)
def to_arrow_identity(s):
    data = s.to_arrow()
    return data


@daft.udf(return_dtype=str)
def to_series_identity(s):
    data = s
    return data


def transform_pylist(df, batch_size=None):
    tfm = to_pylist_identity

    if batch_size is not None:
        tfm = tfm.override_options(batch_size=batch_size)

    df = df.with_column("c", tfm(df["b"]))
    return df


def transform_arrow(df, batch_size=None):
    tfm = to_arrow_identity

    if batch_size is not None:
        tfm = tfm.override_options(batch_size=batch_size)

    df = df.with_column("c", tfm(df["b"]))
    return df


def transform_series(df, batch_size=None):
    tfm = to_series_identity

    if batch_size is not None:
        tfm = tfm.override_options(batch_size=batch_size)

    df = df.with_column("c", tfm(df["b"]))
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--transform", choices=["arrow", "pylist", "series"], default="series")
    parser.add_argument("--batch-size", default=None, type=int)
    args = parser.parse_args()

    if args.transform == "series":
        transform = transform_series
    elif args.transform == "pylist":
        transform = transform_pylist
    elif args.transform == "arrow":
        transform = transform_arrow
    else:
        raise ValueError(f"Unrecignized transform {args.transform}")

    df = daft.read_parquet("tests/assets/parquet-data/mvp.parquet")

    for i in range(128):
        df = df.concat(daft.read_parquet("tests/assets/parquet-data/mvp.parquet"))

    df = df.transform(transform, batch_size=args.batch_size)
    df.collect()
    print(df)
