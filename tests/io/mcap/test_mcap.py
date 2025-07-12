from __future__ import annotations

import pyarrow as pa
import pytest
from mcap_ros2.writer import Writer

import daft
from daft.filesystem import _infer_filesystem
from daft.io import IOConfig, S3Config

try:
    import tosfs  # noqa: F401

    HAS_TOSFS = True
except ImportError:
    HAS_TOSFS = False

data = pa.Table.from_arrays([pa.array([f"chatter test #{i}" for i in range(10)])], names=["data"])

io_config = IOConfig(
    s3=S3Config(
        endpoint_url="tos-s3-cn-beijing.ivolces.com",
        force_virtual_addressing=True,
        key_id="xx",
        access_key="xx==",
        verify_ssl=True,
        region_name="cn-beijing",
    )
)


@pytest.fixture(scope="function")
def mcap_dataset_path(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("mcap")
    file_path = tmp_dir / "test.mcap"

    with open(file_path, "wb") as f:
        writer = Writer(f)
        schema = writer.register_msgdef(datatype="std_msgs/msg/String", msgdef_text="string data")

        for i in range(100):
            writer.write_message(
                topic="/test_topic",
                schema=schema,
                message={"data": f"Chatter #{i}", "antother": f"Another field {i}"},
                log_time=i * 100,
                publish_time=i * 100,
                sequence=i,
            )
        writer.finish()

    yield file_path


@pytest.fixture(scope="function")
def data_from_s3():
    s3_file_path = "s3://kamui/las/mcap/test.mcap"
    file_path, fs, _ = _infer_filesystem(s3_file_path, io_config)
    print(f"Writing MCAP file to S3 at {file_path}, fs: {fs},")
    with fs.open_output_stream(file_path) as f:
        writer = Writer(f)
        schema = writer.register_msgdef(datatype="std_msgs/msg/String", msgdef_text="string data")

        for i in range(100):
            writer.write_message(
                topic="/test_topic",
                schema=schema,
                message={"data": f"Chatter #{i}", "antother": f"Another field {i}"},
                log_time=i * 100,
                publish_time=i * 100,
                sequence=i,
            )
        writer.finish()

    yield s3_file_path


@pytest.mark.parametrize("mcap_dataset_path", ["mcap_dataset_path"], indirect=True)
def test_mcap_read(mcap_dataset_path):
    df = daft.read_mcap(mcap_dataset_path, start_time=0, end_time=100000, topics=["/test_topic"])
    df = df.collect()

    pdf = df.to_pandas()

    assert len(pdf) == 100
    assert "topic" in pdf.columns
    assert "data" in pdf.columns
    assert pdf["publish_time"].between(0, 9900).all()


@pytest.mark.skipif(not HAS_TOSFS, reason="TOSFS not installed, skip S3 tests")
@pytest.mark.parametrize("data_from_s3", ["data_from_s3"], indirect=True)
def test_mcap_read_s3(data_from_s3):
    df = daft.read_mcap(data_from_s3, start_time=0, end_time=100000, topics=["/test_topic"], io_config=io_config)
    df = df.collect()

    pdf = df.to_pandas()

    assert len(pdf) == 100
    assert pdf["sequence"].nunique() == 100
    assert pdf["data"].str.startswith("Chatter #").all()


@pytest.mark.skipif(not HAS_TOSFS, reason="TOSFS not installed, skip S3 tests")
def test_mcap_with_json_protobuf_read():
    tos_ak = "xx"
    tos_sk = "xx=="
    tos_region = "cn-beijing"
    io_config = IOConfig(
        s3=S3Config(
            endpoint_url="tos-s3-cn-beijing.ivolces.com",
            force_virtual_addressing=True,
            key_id=tos_ak,
            access_key=tos_sk,
            verify_ssl=True,
            region_name=tos_region,
        )
    )
    mcap_dataset_path = "s3://hu-las/mcap_test/demo_2025-07-05_15-06-26.mcap"
    df = daft.read_mcap(mcap_dataset_path, io_config=io_config)
    df = df.collect()
    df.show()
