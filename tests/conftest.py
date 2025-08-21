import os
from pathlib import Path

import pytest

import pfeed as pe


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
        "record_mode": 'once',
        # Matchers decide how requests are identified in a cassette
        "match_on": ["method", "scheme", "host", "port", "path", "query", "body"],
        # Redact secrets
        # "filter_headers": [("authorization", "DUMMY")],
    }

@pytest.fixture(scope="session", autouse=True)
def configure_test_env():
    # Disable tqdm globally
    os.environ["TQDM_DISABLE"] = "1"
    # Silence logs below WARNING
    pe.configure(logging_config={
        "handlers": {"stream_handler": {"level": "WARNING"}}
    })