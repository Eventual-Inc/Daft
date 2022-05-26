import tempfile

from daft.dataclasses import dataclass
from daft.datarepo.log import DaftLakeLog


def _create_log(path: str) -> None:
    @dataclass
    class TestSchema:
        x: int
        name: str

    log = DaftLakeLog(path)
    log.create(TestSchema._daft_schema.arrow_schema())

    log.start_transaction("sammy")
    log.add_file("test/add_one_file")
    log.commit()

    log.start_transaction("sammy")
    for i in range(5):
        log.add_file(f"test/add_many_file_{i}")
    log.commit()

    log.start_transaction("sammy")
    log.remove_file("test/add_one_file")
    log.commit()
    first_table = log.history().to_arrow_table().to_pandas()
    del log

    log2 = DaftLakeLog(path)
    second_table = log2.history().to_arrow_table().to_pandas()

    assert first_table.equals(second_table)


def test_create_log_in_memory():
    _create_log("memory://")


def test_create_log_local_fs():
    with tempfile.TemporaryDirectory() as tmpdirname:
        print(tmpdirname)
        _create_log(f"file://{tmpdirname}")
