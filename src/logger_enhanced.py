"""
Logging estruturado com correlation IDs.

Sinks configurados:
  stderr         — colorido, nível configurável
  logs/pipeline.log — todos os eventos em texto, rotação 100 MB, retenção 30 dias
"""

from __future__ import annotations

import sys
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger as loguru_logger

from src.config import LOGS_DIR

_setup_done = False


class StructuredLogger:
    """Injeta correlation ID e nome de operação em cada log via loguru bind."""

    def __init__(self):
        self._local = threading.local()

    def set_correlation_id(self, cid: str | None = None) -> str:
        if cid is None:
            cid = str(uuid.uuid4())
        self._local.correlation_id = cid
        return cid

    def get_correlation_id(self) -> str | None:
        return getattr(self._local, "correlation_id", None)

    def set_operation(self, operation: str) -> None:
        self._local.operation = operation

    def get_operation(self) -> str | None:
        return getattr(self._local, "operation", None)

    @contextmanager
    def operation_context(self, operation: str, cid: str | None = None):
        """Salva/restaura operation e correlation_id por thread."""
        old_op  = self.get_operation()
        old_cid = self.get_correlation_id()

        self.set_operation(operation)
        if cid:
            self.set_correlation_id(cid)
        elif not old_cid:
            self.set_correlation_id()

        try:
            yield self.get_correlation_id()
        finally:
            self._local.operation  = old_op
            self._local.correlation_id = old_cid

    def _extra(self, **kwargs) -> Dict[str, Any]:
        return {
            "correlation_id": self.get_correlation_id(),
            "operation":      self.get_operation(),
            **kwargs,
        }

    def debug(self, message: str, *args, **kwargs) -> None:
        if args:
            message = message.format(*args)
        loguru_logger.bind(**self._extra(**kwargs)).debug(message)

    def info(self, message: str, *args, **kwargs) -> None:
        if args:
            message = message.format(*args)
        loguru_logger.bind(**self._extra(**kwargs)).info(message)

    def success(self, message: str, *args, **kwargs) -> None:
        if args:
            message = message.format(*args)
        loguru_logger.bind(**self._extra(**kwargs)).success(message)

    def warning(self, message: str, *args, **kwargs) -> None:
        if args:
            message = message.format(*args)
        loguru_logger.bind(**self._extra(**kwargs)).warning(message)

    def error(self, message: str, *args, **kwargs) -> None:
        if args:
            message = message.format(*args)
        loguru_logger.bind(**self._extra(**kwargs)).error(message)

    def exception(self, message: str, *args, **kwargs) -> None:
        if args:
            message = message.format(*args)
        loguru_logger.bind(**self._extra(**kwargs)).exception(message)


# Instância global
structured_logger = StructuredLogger()


def setup_enhanced_logging(
    level: str = "INFO",
    log_dir: Path = LOGS_DIR,
) -> None:
    """
    Configura loguru com dois sinks:
      - stderr:        human-readable colorido
      - pipeline.log:  todos os eventos, rotação 100 MB, retenção 30 dias

    Idempotente: ignorado se já configurado nesta sessão.
    """
    global _setup_done
    if _setup_done:
        return
    _setup_done = True

    loguru_logger.remove()

    # Console — legível por humanos
    loguru_logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{line}</cyan> — <level>{message}</level>"
        ),
        colorize=True,
    )

    # Arquivo — texto legível, rotation automática
    loguru_logger.add(
        str(log_dir / "pipeline.log"),
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{line} — {message}",
        rotation="100 MB",
        retention="30 days",
        compression="gz",
        encoding="utf-8",
    )


# Expõe loguru diretamente para uso nos módulos
logger = loguru_logger


__all__ = [
    "StructuredLogger",
    "structured_logger",
    "logger",
    "setup_enhanced_logging",
]
