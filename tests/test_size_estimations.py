from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

from daft.daft import testing as native_testing_utils
from daft.recordbatch.micropartition import MicroPartition

pytest.skip(allow_module_level=True, reason="Skipping because these tests don't currently pass")


def get_scantask_estimated_size(pq_path: str, size_on_disk: int, columns: list[str] | None = None) -> int:
    """Retrieve the estimated size for reading a given Parquet file."""
    return native_testing_utils.estimate_in_memory_size_bytes(str(pq_path), size_on_disk, columns=columns)


def get_actual_size(pq_path: str, columns: list[str] | None = None) -> int:
    # Force materializationm of the MicroPartition using `.slice`
    mp = MicroPartition.read_parquet(str(pq_path), columns=columns)
    return mp.slice(0, len(mp)).size_bytes()


def assert_close(size_on_disk, estimated: int, actual: int, pct: float = 0.3):
    assert (
        abs(actual - estimated) / estimated < pct
    ), f"Expected {size_on_disk / 1_000_000:.2f}MB file estimated vs actual to be within {pct * 100}%: {estimated / 1_000_000:.2f}MB vs {actual / 1000_000:.2f}MB ({((estimated - actual) / estimated) * 100:.2f}%)"


@pytest.mark.parametrize("compression", ["snappy", None], ids=["snappy", "no_compression"])
@pytest.mark.parametrize("use_dictionary", [True, False], ids=["dict", "no_dict"])
def test_estimations_unique_strings(tmpdir, use_dictionary, compression):
    pq_path = tmpdir / f"unique_strings.use_dictionary={use_dictionary}.compression={compression}.pq"
    data = [f"{'a' * 100}{i}" for i in range(1000_000)]
    tbl = pa.table({"foo": data})
    papq.write_table(tbl, pq_path, use_dictionary=use_dictionary, compression=compression)

    size_on_disk = pq_path.stat().size
    assert_close(size_on_disk, get_scantask_estimated_size(pq_path, size_on_disk), get_actual_size(pq_path))


@pytest.mark.parametrize("compression", ["snappy", None], ids=["snappy", "no_compression"])
@pytest.mark.parametrize("use_dictionary", [True, False], ids=["dict", "no_dict"])
def test_estimations_dup_strings(tmpdir, use_dictionary, compression):
    pq_path = tmpdir / f"dup_strings.use_dictionary={use_dictionary}.compression={compression}.pq"
    data = ["a" * 100 for _ in range(1000_000)]
    tbl = pa.table({"foo": data})
    papq.write_table(tbl, pq_path, use_dictionary=use_dictionary, compression=compression)

    size_on_disk = pq_path.stat().size
    assert_close(size_on_disk, get_scantask_estimated_size(pq_path, size_on_disk), get_actual_size(pq_path))


@pytest.mark.parametrize("compression", ["snappy", None], ids=["snappy", "no_compression"])
@pytest.mark.parametrize("use_dictionary", [True, False], ids=["dict", "no_dict"])
def test_estimations_unique_ints(tmpdir, use_dictionary, compression):
    pq_path = tmpdir / f"unique_ints.use_dictionary={use_dictionary}.compression={compression}.pq"

    data = [i for i in range(1000_000)]
    tbl = pa.table({"foo": data})
    papq.write_table(tbl, pq_path, use_dictionary=use_dictionary, compression=compression)

    size_on_disk = pq_path.stat().size
    assert_close(size_on_disk, get_scantask_estimated_size(pq_path, size_on_disk), get_actual_size(pq_path))


@pytest.mark.parametrize("compression", ["snappy", None], ids=["snappy", "no_compression"])
@pytest.mark.parametrize("use_dictionary", [True, False], ids=["dict", "no_dict"])
def test_estimations_dup_ints(tmpdir, use_dictionary, compression):
    pq_path = tmpdir / f"dup_ints.use_dictionary={use_dictionary}.compression={compression}.pq"

    data = [1 for _ in range(1000_000)]
    tbl = pa.table({"foo": data})
    papq.write_table(tbl, pq_path, use_dictionary=use_dictionary, compression=compression)

    size_on_disk = pq_path.stat().size
    assert_close(size_on_disk, get_scantask_estimated_size(pq_path, size_on_disk), get_actual_size(pq_path))


@pytest.mark.parametrize(
    "path",
    [
        "https://huggingface.co/datasets/HuggingFaceTB/smoltalk/resolve/main/data/all/test-00000-of-00001.parquet",
        "https://huggingface.co/datasets/uoft-cs/cifar10/resolve/main/plain_text/test-00000-of-00001.parquet",
        "https://huggingface.co/datasets/stanfordnlp/imdb/resolve/main/plain_text/unsupervised-00000-of-00001.parquet",
        "https://huggingface.co/datasets/jat-project/jat-dataset-tokenized/resolve/main/atari-alien/test-00000-of-00011.parquet",
        "https://huggingface.co/datasets/princeton-nlp/SWE-bench_Verified/resolve/main/data/test-00000-of-00001.parquet",
        "https://huggingface.co/datasets/lmms-lab/LLaVA-OneVision-Data/resolve/main/CLEVR-Math(MathV360K)/train-00000-of-00002.parquet",
    ],
    ids=[
        "smoktalk",
        "cifar10",
        "standfordnlp-imdb",
        "jat-dataset-tokenized",
        "swe-bench-verified",
        "llava-onevision-data",
    ],
)
def test_canonical_files_in_hf(path):
    import requests

    response = requests.head(path, allow_redirects=True)
    size_on_disk = int(response.headers["Content-Length"])

    assert_close(size_on_disk, get_scantask_estimated_size(path, size_on_disk), get_actual_size(path))


@pytest.mark.parametrize(
    "path",
    [
        "s3://daft-public-datasets/tpch_iceberg_sf1000.db/lineitem/data/L_SHIPDATE_month=1992-01/00000-6694-fa4594d5-f624-407c-8640-5b6db8150470-00001.parquet",
    ],
    ids=[
        "lineitem",
    ],
)
def test_canonical_files_in_s3(path):
    import boto3

    s3 = boto3.client("s3")
    bucket, key = path.replace("s3://", "").split("/", 1)
    response = s3.head_object(Bucket=bucket, Key=key)
    size_on_disk = response["ContentLength"]

    assert_close(size_on_disk, get_scantask_estimated_size(path, size_on_disk), get_actual_size(path))
