import argparse

import pytest
import ray


@pytest.fixture(scope="session")
def ray_cluster():
    ray.init(num_cpus=2)
    yield
    ray.shutdown()


def pytest_addoption(parser):
    parser.addoption("--run_conda", action="store_true", default=False, help="run tests that require conda")
    parser.addoption("--run_docker", action="store_true", default=False, help="run tests that require docker")
    parser.addoption(
        "--run_tdd", action="store_true", default=False, help="run tests that are marked for Test Driven Development"
    )
    parser.addoption(
        "--run_tdd_all",
        action="store_true",
        default=False,
        help="run tests that are marked for Test Driven Development (including low priority)",
    )
    parser.addoption("--run_tpch", action="store_true", default=False, help="run tcp-h tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "conda: mark test as requiring conda to run")
    config.addinivalue_line("markers", "docker: mark test as requiring docker to run")
    config.addinivalue_line("markers", "tdd: mark test as for TDD in active development")
    config.addinivalue_line("markers", "tdd_all: mark test as for TDD but not in active development")
    config.addinivalue_line("markers", "tpch: mark as a tpch test")


def pytest_collection_modifyitems(config, items):
    marks = {
        "conda": pytest.mark.skip(reason="need --run_conda option to run"),
        "docker": pytest.mark.skip(reason="need --run_docker option to run"),
        "tdd": pytest.mark.skip(reason="need --run_tdd option to run"),
        "tdd_all": pytest.mark.skip(reason="need --run_tdd_all option to run"),
        "tpch": pytest.mark.skip(reason="need --run_tpch option to run"),
    }
    for item in items:
        for keyword in marks:
            if keyword in item.keywords and not config.getoption(f"--run_{keyword}"):
                item.add_marker(marks[keyword])


def run_tdd():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_tdd", action="store_true")
    args, _ = parser.parse_known_args()
    return args.run_tdd
