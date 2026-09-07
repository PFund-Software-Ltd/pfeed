from __future__ import annotations

from collections.abc import Callable
from typing import Any


def is_lambda(func: Callable[..., Any]) -> bool:
    import types

    return isinstance(func, types.LambdaType) and func.__name__ == "<lambda>"


def lambda_with_name(name: str, lambda_func: Callable[..., Any]):
    lambda_func.__name__ = name
    return lambda_func


def is_using_prefect() -> bool:
    """Whether batch dataflows should run as Prefect flows, per `pfeed.configure(use_prefect=...)`.

    Raises:
        ImportError: if use_prefect is enabled but the `prefect` package is not installed.
    """
    from importlib.util import find_spec

    from pfeed.config import get_config

    if not get_config().use_prefect:
        return False
    if find_spec("prefect") is None:
        raise ImportError(
            "use_prefect is enabled but `prefect` is not installed. "
            'Install it with `pip install "pfeed[prefect]"`.'
        )
    return True
