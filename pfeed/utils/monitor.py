import psutil

from pfund import cprint


def print_disk_usage(path: str | None=None) -> None:
    """
    Prints the disk usage of the given path.
    """
    if not path:
        from pfeed.config import get_config
        config = get_config()
        path = config.data_path
    disk_usage = psutil.disk_usage(path)
    cprint(f"Disk usage at {path}: {disk_usage.percent}%", style="bold red")


def print_ram_usage() -> None:
    """
    Prints the system's RAM usage.
    """
    ram_usage = psutil.virtual_memory()
    cprint(f"RAM usage: {ram_usage.percent}%", style="bold red")
