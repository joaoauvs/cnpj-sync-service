"""
Centralised logging configuration using loguru.

Import `logger` from here instead of configuring it per-module.
"""

from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger

from src.config import LOGS_DIR


def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """
    Configure loguru for the pipeline.

    Args:
        level: Minimum log level (DEBUG, INFO, WARNING, ERROR).
        log_file: Optional explicit log file path. Defaults to
                  logs/pipeline_<date>.log.
    """
    logger.remove()

    # Console — human-friendly
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{line}</cyan> — <level>{message}</level>"
        ),
        colorize=True,
    )

    # File — full detail for post-mortem analysis
    if log_file is None:
        from datetime import date
        log_file = str(LOGS_DIR / f"pipeline_{date.today().isoformat()}.log")

    logger.add(
        log_file,
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{line} — {message}",
        rotation="100 MB",
        retention="30 days",
        compression="gz",
        encoding="utf-8",
    )

    logger.info("Logging initialised — level={}, file={}", level, log_file)


__all__ = ["logger", "setup_logging"]
