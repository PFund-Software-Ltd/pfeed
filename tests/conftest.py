from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfund_blocks.entities.products.product_base import BaseProduct

import os

import pytest

import pfeed as pe
from pfund_kit.utils.ray import shutdown_ray


def pytest_addoption(parser: pytest.Parser):
    parser.addoption(
        "--keep-log-path",
        "--kl",
        action="store_true",
        help="write pfeed logs to the configured log_path instead of a tmp dir",
    )


def pytest_configure(config: pytest.Config):
    # xdist workers would compete to rotate the same log files in the real log_path
    if config.getoption("keep_log_path") and config.getoption("numprocesses", default=None):
        raise pytest.UsageError("--keep-log-path can't be used with -n (workers would share log files)")


# TODO: add more products (e.g. stocks) to params once they are supported
@pytest.fixture(params=["bybit"])
def product(request: pytest.FixtureRequest) -> BaseProduct:
    if request.param == "bybit":
        from pfund.venues.bybit.venue import Bybit
        return Bybit.create_product("BTC_USDT_PERP")
    raise ValueError(f"unknown product param: {request.param}")


@pytest.fixture(scope="session", autouse=True)
def configure_test_env(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory):
    # disable this warning: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default).
    os.environ["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"
    # redirect log files to a temp dir; xdist gives each worker its own basetemp,
    # so workers never rotate the same log file
    if not request.config.getoption("keep_log_path"):
        pe.configure(log_path=tmp_path_factory.mktemp("logs"))


@pytest.fixture(scope="function", autouse=True)
def cleanup_ray():
    """Shut down Ray after each test, so no test inherits another test's Ray instance."""
    yield
    shutdown_ray()
