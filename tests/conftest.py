from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfund_blocks.entities.products.product_base import BaseProduct

import os

import pytest
from pfund_kit.utils.ray import shutdown_ray

import pfeed as pe
from pfeed.config import setup_logging


def pytest_addoption(parser: pytest.Parser):
    parser.addoption(
        "--keep-log-path",
        "--kl",
        action="store_true",
        help="write pfeed logs to the configured log_path instead of a tmp dir",
    )


def pytest_configure(config: pytest.Config):
    # xdist workers would compete to rotate the same log files in the real log_path
    if config.getoption("keep_log_path") and config.getoption(
        "numprocesses", default=None
    ):
        raise pytest.UsageError(
            "--keep-log-path can't be used with -n (workers would share log files)"
        )


# TODO: add more products (e.g. stocks) to params once they are supported
@pytest.fixture(params=["bybit"])
def product(request: pytest.FixtureRequest) -> BaseProduct:
    if request.param == "bybit":
        from pfund_blocks.entities.products.product_factory import ProductFactory

        basis = "BTC_USDT_PERP"
        return ProductFactory(basis).model_validate(
            {"source": "BYBIT", "basis": basis, "symbol": "BTCUSDT"}
        )
    raise ValueError(f"unknown product param: {request.param}")


@pytest.fixture(scope="session", autouse=True)
def configure_test_env(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
):
    # disable this warning: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default).
    os.environ["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"
    # redirect log files to a temp dir; xdist gives each worker its own basetemp,
    # so workers never rotate the same log file
    if not request.config.getoption("keep_log_path"):
        pe.configure(log_path=tmp_path_factory.mktemp("logs"))
    # before any test creates a "pfeed.<source>" logger, e.g. a data handler created without a feed,
    # otherwise it is created as a plain Logger instead of pfeed's ColoredLogger, and the feeds of that source get it too
    setup_logging()


@pytest.fixture(autouse=True)
def start_ray(request: pytest.FixtureRequest):
    """Starts Ray before a test marked xdist_group("ray"), retrying a startup that timed out.

    Ray waits a hardcoded 30s for its node to register (ray/_private/node.py, not configurable),
    and on a slow, busy CI runner (e.g. windows-latest, other xdist workers running) it can miss it.
    pfeed's setup_ray() then finds Ray running and doesn't start it again.
    """
    marker = request.node.get_closest_marker("xdist_group")
    if marker is None or marker.args != ("ray",):
        return
    try:
        import ray
    except ImportError:
        return  # the test skips itself with pytest.importorskip("ray")
    num_attempts = 3
    for attempt in range(1, num_attempts + 1):
        try:
            # same as pfund_kit's setup_ray()
            ray.init(num_cpus=os.cpu_count())
            return
        except Exception as exc:
            if "timed out during startup" not in str(exc) or attempt == num_attempts:
                raise
            # clean up the half-started processes before trying again
            shutdown_ray(wait_for_processes=True)


@pytest.fixture(scope="function", autouse=True)
def cleanup_ray():
    """Shut down Ray after each test, so no test inherits another test's Ray instance."""
    yield
    shutdown_ray()
