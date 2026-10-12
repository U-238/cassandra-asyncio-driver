import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration", action="store_true", help="Run tests requiring Cassandra"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration"):
        return
    skip = pytest.mark.skip(
        reason="Use --run-integration with a local Cassandra server"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
