def run_cli() -> None:
    """Application Entrypoint."""
    from pfeed.cli import pfeed_group
    pfeed_group(obj={})


if __name__ == '__main__':
    run_cli()
