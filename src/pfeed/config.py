# pyright: reportUnusedParameter=false
from __future__ import annotations

from pathlib import Path
from typing import Any

from pfund.enums.env import Environment
from pfund_kit import logging as kit_logging
from pfund_kit.config import Configuration

from pfeed.enums import DataTool

__all__ = [
    "configure",
    "configure_logging",
    "get_config",
    "setup_logging",
]


project_name = "pfeed"
_config: PFeedConfig | None = None


def setup_logging(env: Environment | str | None = None, reset: bool = False) -> None:
    env = Environment[env.upper()] if env else None
    kit_logging.setup_logging(get_config(), env=env, reset=reset)


def get_config() -> PFeedConfig:
    """Lazy singleton - only creates config when first called.
    Also loads the .env file.
    """
    global _config
    if _config is None:
        _config = PFeedConfig()
    return _config


def get_logging_config() -> dict[str, Any]:
    return kit_logging.get_logging_config(get_config())


def configure(
    data_path: str | None = None,
    log_path: str | None = None,
    cache_path: str | None = None,
    data_tool: DataTool | str | None = None,
    persist: bool = False,
) -> PFeedConfig:
    """
    Configures the global config object.
    Args:
        data_path: Path to the data directory.
        log_path: Path to the log directory.
        cache_path: Path to the cache directory.
        data_tool: Data tool to use, e.g. pandas, polars, etc.
        persist: If True, the config will be saved to the config file.
    """
    config = get_config()
    config_dict = config.to_dict()
    config_dict.pop("__version__")

    # Apply updates for non-None values
    for k in config_dict:
        v = locals().get(k)
        if v is not None:
            if "_path" in k:
                v = Path(v)
            elif k == "data_tool":
                v = DataTool[v.lower()]
            setattr(config, k, v)

    config.ensure_dirs()

    if persist:
        config.save()

    return config


def configure_logging(
    logging_config: dict[str, Any] | None = None, debug: bool = False
) -> dict[str, Any]:
    return kit_logging.configure_logging(
        get_config(), overrides=logging_config, debug=debug
    )


class PFeedConfig(Configuration):
    def __init__(self):
        from pfund_kit.utils import load_env_file

        _ = load_env_file(verbose=False)
        super().__init__(project_name=project_name, source_file=__file__)

    def _initialize_from_data(self):
        """Initialize PFeedConfig-specific attributes from config data."""
        self.data_tool = DataTool[self._data.get("data_tool", DataTool.polars).lower()]

    def to_dict(self) -> dict[str, Any]:
        return {
            **super().to_dict(),
            "data_tool": self.data_tool,
        }

    def prepare_docker_context(self):
        """Prepare docker context (e.g. env variables) before running compose.yml"""
        pass
