from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator

    from ray.util.queue import Queue

import logging
import threading
from contextlib import contextmanager
from logging.handlers import QueueHandler, QueueListener

# Callers reach `setup_ray` from worker threads, so the check and the init have
# to be one step: two threads finding Ray uninitialized both go on to start it,
# and the one that gets there second fails the `ray.init` twice assertion.
_INIT_LOCK = threading.Lock()


def get_ray_num_cpus(self) -> int:
    """Get the number of CPUs available in the Ray cluster."""
    import ray

    if not ray.is_initialized():
        raise RuntimeError("Ray must be initialized before getting the number of CPUs")
    cluster_resources = ray.cluster_resources()
    return int(cluster_resources.get("CPU", 0))


def setup_ray():
    import atexit
    import os

    import ray
    from pfund_kit.style import RichColor, TextStyle, cprint

    with _INIT_LOCK:
        if not is_ray_initialized():
            cprint(
                f"Auto-initializing Ray with {os.cpu_count()} CPUs",
                style=TextStyle.BOLD + RichColor.YELLOW,
            )
            ray.init(num_cpus=os.cpu_count())
            atexit.register(
                shutdown_ray, wait_for_processes=True
            )  # useful in jupyter notebook environment


def is_ray_initialized() -> bool:
    import sys

    # Ray cannot be running if nothing imported it; skip the slow import.
    if "ray" not in sys.modules:
        return False
    import ray

    return ray.is_initialized()


def shutdown_ray(wait_for_processes: bool = False):
    if not is_ray_initialized():
        return
    import ray

    ray.shutdown(wait_for_processes=wait_for_processes)


def setup_logger_in_ray_task(logger_name: str, log_queue: Queue) -> logging.Logger:
    """Configure a logger with QueueHandler in a Ray task.

    Args:
        logger_name: Name of the logger to create/get
        log_queue: Ray Queue for logging

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(logger_name)
    if not logger.handlers:
        logger.addHandler(QueueHandler(log_queue))
        logger.setLevel(logging.DEBUG)
        # needs this to avoid triggering the root logger's stream handlers with level=DEBUG
        logger.propagate = False
    return logger


@contextmanager
def ray_logging_context(logger: logging.Logger) -> Generator[Queue, None, None]:
    """Context manager for Ray logging setup with QueueListener.

    Yields:
        Ray Queue for logging that workers can use with QueueHandler
    """
    from ray.util.queue import Queue

    log_queue = Queue()
    log_listener = QueueListener(
        log_queue, *logger.handlers, respect_handler_level=True
    )
    log_listener.start()
    try:
        yield log_queue
    finally:
        log_listener.stop()
