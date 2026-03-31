"""Centralized logging configuration and logger factory for workflow runtime."""

from __future__ import annotations

import logging
import os
from typing import Final


DEFAULT_LOG_LEVEL: Final[str] = "INFO"
DEFAULT_LOG_FORMAT: Final[str] = "%(message)s"
DEFAULT_LOG_DATE_FORMAT: Final[str] = "%Y-%m-%dT%H:%M:%S%z"


# SEM_BEGIN orchestrator_v1.runtime_logging.normalize_log_level:v1
# type: METHOD
# use_case: Normalizes runtime logging level input into the integer level expected by Python logging.
# feature:
#   - Runtime logging configuration should accept explicit values and env-backed defaults without duplicating level parsing
# pre:
#   - level is None, a valid logging level string, or a logging level integer
# post:
#   - returns a logging level integer accepted by logging.basicConfig
# invariant:
#   - process environment is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - ValueError: level string is not a supported logging level
# depends:
#   - logging.getLevelNamesMapping
# sft: normalize runtime logging level input into a valid Python logging level integer
# idempotent: true
# logs: -
def _normalize_log_level(level: str | int | None) -> int:
    if isinstance(level, int):
        return level

    resolved_level = str(level or os.getenv("WORKFLOW_RUNTIME_LOG_LEVEL", DEFAULT_LOG_LEVEL)).upper()
    mapping = logging.getLevelNamesMapping()
    if resolved_level not in mapping:
        raise ValueError(f"Unsupported logging level: {resolved_level}")
    return mapping[resolved_level]


# SEM_END orchestrator_v1.runtime_logging.normalize_log_level:v1


# SEM_BEGIN orchestrator_v1.runtime_logging.configure_logging:v1
# type: METHOD
# use_case: Applies centralized workflow runtime logging settings to the Python root logger.
# feature:
#   - Runtime modules should share one place for log level and formatter control instead of configuring logging ad hoc
#   - AFL message format stays inside log messages while the outer logger settings remain configurable from one module
# pre:
#   - level is None, a valid logging level string, or a logging level integer
# post:
#   - root logger is configured or updated with the requested runtime logging settings
# invariant:
#   - existing handlers are preserved unless force is true
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - ValueError: pre[0] violated
# depends:
#   - logging.basicConfig
#   - _normalize_log_level
# sft: configure centralized Python logging settings for the workflow runtime from one module
# idempotent: false
# logs: -
def configure_logging(
    *,
    level: str | int | None = None,
    fmt: str | None = None,
    datefmt: str | None = None,
    force: bool = False,
) -> None:
    resolved_level = _normalize_log_level(level)
    resolved_format = fmt or os.getenv("WORKFLOW_RUNTIME_LOG_FORMAT", DEFAULT_LOG_FORMAT)
    resolved_date_format = datefmt or os.getenv("WORKFLOW_RUNTIME_LOG_DATE_FORMAT", DEFAULT_LOG_DATE_FORMAT)
    root_logger = logging.getLogger()

    if root_logger.handlers and not force:
        root_logger.setLevel(resolved_level)
        return

    logging.basicConfig(
        level=resolved_level,
        format=resolved_format,
        datefmt=resolved_date_format,
        force=force,
    )


# SEM_END orchestrator_v1.runtime_logging.configure_logging:v1


# SEM_BEGIN orchestrator_v1.runtime_logging.get_logger:v1
# type: METHOD
# use_case: Returns a module logger after ensuring centralized runtime logging is initialized.
# feature:
#   - Runtime modules should obtain logger instances through one shared factory so configuration can be managed centrally
# pre:
#   - name is not empty
# post:
#   - returns a logger for the requested module name
# invariant:
#   - logger identity remains keyed by the provided name
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - ValueError: name is empty
# depends:
#   - configure_logging
#   - logging.getLogger
# sft: return a module logger through a centralized workflow runtime logging factory
# idempotent: true
# logs: -
def get_logger(name: str) -> logging.Logger:
    if not name:
        raise ValueError("Logger name must not be empty")

    if not logging.getLogger().handlers:
        configure_logging()
    return logging.getLogger(name)


# SEM_END orchestrator_v1.runtime_logging.get_logger:v1
