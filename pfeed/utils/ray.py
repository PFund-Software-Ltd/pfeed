from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from typing import Generator
    from ray.util.queue import Queue

import logging
from logging.handlers import QueueHandler, QueueListener
from contextlib import contextmanager


def get_ray_num_cpus(self) -> int:
    """Get the number of CPUs available in the Ray cluster."""
    import ray
    if not ray.is_initialized():
        raise RuntimeError('Ray must be initialized before getting the number of CPUs')
    cluster_resources = ray.cluster_resources()
    return int(cluster_resources.get('CPU', 0))


def setup_ray():
    import os
    import ray
    import atexit
    from pfund_kit.style import cprint, TextStyle, RichColor
    if not ray.is_initialized():
        ray.init(num_cpus=os.cpu_count())
        cprint(f'Auto-initialized Ray with {os.cpu_count()} CPUs', style=TextStyle.BOLD + RichColor.YELLOW)
        atexit.register(lambda: ray.shutdown())  # useful in jupyter notebook environment


def shutdown_ray():
    import ray
    if ray.is_initialized():
        ray.shutdown()
            

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
    log_listener = QueueListener(log_queue, *logger.handlers, respect_handler_level=True)
    log_listener.start()
    try:
        yield log_queue
    finally:
        log_listener.stop()
 