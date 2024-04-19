import atexit

from pfeed.cli import pfeed_group


def exit_cli():
    """Application Exitpoint."""
    print("Cleanup actions here...")


def run_cli() -> None:
    """Application Entrypoint."""
    # atexit.register(exit_cli)
    pfeed_group(obj={})


if __name__ == '__main__':
    run_cli()