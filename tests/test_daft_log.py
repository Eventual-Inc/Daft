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

    print(f"\n{log.history().to_arrow_table().to_pandas()}")
