from __future__ import annotations
from typing import Callable, Any


def is_lambda(func: Callable[..., Any]) -> bool:
    import types
    return isinstance(func, types.LambdaType) and func.__name__ == "<lambda>"


def lambda_with_name(name: str, lambda_func: Callable[..., Any]):
    lambda_func.__name__ = name
    return lambda_func


def is_prefect_running() -> bool:
    import os
    import httpx

    url = os.getenv('PREFECT_API_URL', 'http://127.0.0.1:4200/api').rstrip('/')
    if not url.startswith('http'):
        url = f'http://{url}'
    try:
        response = httpx.get(f'{url}/health', timeout=2.0)
        return response.status_code == 200
    except Exception:
        # Catch all exceptions - if we can't verify Prefect is running, assume it's not
        return False
