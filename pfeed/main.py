from pfeed.cli import pfeed_group


def run_cli() -> None:
    """Application Entrypoint."""
    pfeed_group(obj={})


if __name__ == '__main__':
    run_cli()