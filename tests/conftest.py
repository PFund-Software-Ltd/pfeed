import os
from pathlib import Path

import pytest

import pfeed as pe


@pytest.fixture
def bybit(request):
    return pe.Bybit(**getattr(request, "param", {}))


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


@pytest.fixture(scope="session")
def vcr_config():
    cassette_dir = (Path(__file__).parent / "cassettes").resolve()
    cassette_dir.mkdir(parents=True, exist_ok=True)
    return {
        # Where to store cassettes (relative to test file unless absolute)
        "cassette_library_dir": str(cassette_dir),
        # Default record mode: 'once' is a good CI-safe default
        #   'none'          -> never record; fail if no cassette
        #   'once'          -> record if cassette missing, else replay
        #   'new_episodes'  -> append new interactions, i.e. old recordings are kept, only missing ones are added
        #   'all'           -> always record (overwrites)
        "record_mode": "once",
        # Matchers decide how requests are identified in a cassette
        "match_on": ["method", "scheme", "host", "port", "path", "query", "body"],
        # Redact secrets
        # "filter_headers": [("authorization", "DUMMY")],
    }


@pytest.fixture(scope="session", autouse=True)
def configure_test_env(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory):
    # disable this warning: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default).
    os.environ["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"
    # redirect log files to a temp dir; xdist gives each worker its own basetemp,
    # so workers never rotate the same log file
    if not request.config.getoption("keep_log_path"):
        pe.configure(log_path=tmp_path_factory.mktemp("logs"))
    # # Silence logs below WARNING and disable file handlers entirely for tests
    # pe.configure_logging(
    #     {
    #         "handlers": {"stream_handler": {"level": "WARNING"}},
    #         # File handlers removed to avoid race conditions in parallel tests (pytest-xdist)
    #         # Multiple workers would compete to rotate the same log file, causing FileNotFoundError
    #         "loggers": {
    #             "root": {
    #                 "handlers": ["stream_handler"],  # Remove file handler
    #                 "level": "WARNING",
    #             },
    #             "pfeed": {
    #                 "handlers": ["stream_handler"],  # Remove file handler
    #                 "level": "WARNING",
    #             },
    #         },
    #     }
    # )


@pytest.fixture(scope="function", autouse=True)
def cleanup_ray():
    """Cleanup Ray after each test function to prevent pytest teardown warnings."""
    yield
    try:
        import ray

        if ray.is_initialized():
            # Use _exiting_interpreter=True to avoid late imports during shutdown
            # This prevents "cannot send (already closed?)" errors in pytest teardown
            ray.shutdown(_exiting_interpreter=True)
    except Exception:
        pass
