from __future__ import annotations

from typing import Any

from collections.abc import Callable
from types import FunctionType


def is_lambda(func: Callable[..., Any]) -> bool:
    import types

    return isinstance(func, types.LambdaType) and func.__name__ == "<lambda>"


def lambda_with_name(name: str, lambda_func: FunctionType) -> FunctionType:
    lambda_func.__name__ = name
    return lambda_func
