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
import pytest
from io import StringIO
from omopcloudetl_core.logging import setup_logging, ColorFormatter


@pytest.fixture
def clean_logger():
    """Fixture to get a clean logger instance for each test."""
    logger_name = f"test_logger_{id(clean_logger)}"
    logger = logging.getLogger(logger_name)
    if logger.hasHandlers():
        logger.handlers.clear()
    yield logger
    if logger.hasHandlers():
        logger.handlers.clear()


def test_setup_logging_returns_logger_instance(clean_logger):
    """Tests that setup_logging returns a valid logger instance."""
    logger = setup_logging(level=logging.DEBUG, logger_name=clean_logger.name)
    assert isinstance(logger, logging.Logger)


def test_log_format_includes_thread_name(clean_logger):
    """Tests that the log format correctly includes the thread name."""
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    formatter = ColorFormatter("%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    clean_logger.addHandler(handler)
    clean_logger.setLevel(logging.INFO)

    clean_logger.info("Test message")
    log_output = log_stream.getvalue()

    # The default thread name is 'MainThread'
    assert "[MainThread]" in log_output


def test_setup_logging_is_idempotent(clean_logger):
    """Tests that calling setup_logging multiple times does not add duplicate handlers."""
    setup_logging(logger_name=clean_logger.name)
    assert len(clean_logger.handlers) == 1
    setup_logging(logger_name=clean_logger.name)
    assert len(clean_logger.handlers) == 1


def test_color_formatter_no_color():
    """Tests that the formatter returns a plain message for levels without a color."""
    formatter = ColorFormatter()
    record = logging.LogRecord(
        name="test",
        level=33,  # A custom level with no color
        pathname="test.py",
        lineno=1,
        msg="A message",
        args=(),
        exc_info=None,
    )
    formatted_message = formatter.format(record)
    assert formatted_message == "A message"
