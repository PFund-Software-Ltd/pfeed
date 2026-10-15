from __future__ import annotations

from typing import Any

from collections.abc import Callable


def is_lambda(func: Callable[..., Any]) -> bool:
    import types

    return isinstance(func, types.LambdaType) and func.__name__ == "<lambda>"


def lambda_with_name(name: str, lambda_func: Callable[..., Any]):
    lambda_func.__name__ = name
    return lambda_func

