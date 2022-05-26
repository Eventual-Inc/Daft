from torch import log2_

from daft.dataclasses import dataclass
from daft.datarepo.log import DaftLakeLog


def test_create_log() -> None:
    IN_MEMORY_DIR = "memory://"

    @dataclass
    class TestSchema:
        x: int
        name: str

    log = DaftLakeLog(IN_MEMORY_DIR)
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

    log2 = DaftLakeLog(IN_MEMORY_DIR)
    second_table = log2.history().to_arrow_table().to_pandas()

    assert first_table.equals(second_table)
