import psutil
from rich.console import Console


def print_disk_usage(path) -> None:
    """
    Prints the disk usage of the given path.
    """
    disk_usage = psutil.disk_usage(path)
    Console().print(f"Disk usage at {path}: {disk_usage.percent}%", style="bold red")


def print_ram_usage() -> None:
    """
    Prints the system's RAM usage.
    """
    ram_usage = psutil.virtual_memory()
    Console().print(f"RAM usage: {ram_usage.percent}%", style="bold red")
