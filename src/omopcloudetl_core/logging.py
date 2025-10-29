# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

import logging
import sys
from typing import Optional

import colorama


class ColorFormatter(logging.Formatter):
    """A logging formatter that adds color to the output."""

    LEVEL_COLORS = {
        logging.DEBUG: colorama.Fore.CYAN,
        logging.INFO: colorama.Fore.GREEN,
        logging.WARNING: colorama.Fore.YELLOW,
        logging.ERROR: colorama.Fore.RED,
        logging.CRITICAL: colorama.Fore.MAGENTA,
    }

    def format(self, record: logging.LogRecord) -> str:
        color = self.LEVEL_COLORS.get(record.levelno)
        message = super().format(record)
        if color:
            return color + message + colorama.Style.RESET_ALL
        return message


def setup_logging(level: int = logging.INFO, logger_name: Optional[str] = None) -> logging.Logger:
    """
    Set up a centralized, colorized, and thread-safe logger.

    Args:
        level: The logging level (e.g., logging.INFO).
        logger_name: The name of the logger to configure. If None, configures the root logger.

    Returns:
        The configured logger instance.
    """
    colorama.init(autoreset=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Prevent duplicate handlers if the function is called multiple times
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = ColorFormatter(
            "%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# Initialize a default logger for the package
logger = setup_logging()
