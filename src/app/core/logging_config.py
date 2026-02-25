"""API-only logging configuration. Used when starting the FastAPI app (main.py)."""

import logging
import sys

from src.app.core.config import api_settings


def setup_api_logging() -> None:
    """Configure logging for the API. Call once from main.py on startup."""
    level = logging.DEBUG if api_settings.debug else logging.INFO
    format_str = (
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        if api_settings.env_name != "production"
        else "%(levelname)s %(name)s %(message)s"
    )

    logging.basicConfig(
        level=level,
        format=format_str,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    logging.getLogger("uvicorn").setLevel(level)
    logging.getLogger("src.app").setLevel(level)
