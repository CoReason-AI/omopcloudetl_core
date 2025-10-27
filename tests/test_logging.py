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
import threading
from io import StringIO

import pytest
from colorama import Fore, Style

from omopcloudetl_core.logging import setup_logging


@pytest.fixture
def log_capture():
    """Fixture to capture log output."""
    # This is a simplified mock; for more complex scenarios, caplog is better
    # but requires pytest configuration. This avoids that for simplicity.
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    formatter = logging.Formatter("%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    return stream, handler


def test_setup_logging_returns_configured_logger():
    """Test that setup_logging returns a logger with the correct configuration."""
    # Use a unique name to avoid conflicts with other tests
    logger = setup_logging(level=logging.DEBUG, logger_name="test_setup_logger")
    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) > 0


def test_color_formatter_adds_color_codes():
    """Test that the ColorFormatter correctly adds color codes to log messages."""
    logger = setup_logging(logger_name="test_color_logger")
    # To test color, we need to get the formatter from the handler
    # This is a bit of an integration test
    formatter = logger.handlers[0].formatter

    # Test INFO level
    info_record = logging.LogRecord("test", logging.INFO, "/path", 1, "Info message", (), None)
    formatted_info = formatter.format(info_record)
    assert formatted_info.startswith(Fore.GREEN)
    assert "Info message" in formatted_info
    assert formatted_info.endswith(Style.RESET_ALL)

    # Test ERROR level
    error_record = logging.LogRecord("test", logging.ERROR, "/path", 1, "Error message", (), None)
    formatted_error = formatter.format(error_record)
    assert formatted_error.startswith(Fore.RED)
    assert "Error message" in formatted_error
    assert formatted_error.endswith(Style.RESET_ALL)


def test_setup_logging_is_idempotent():
    """Test that calling setup_logging multiple times for the same logger doesn't add more handlers."""
    logger_name = "idempotent_logger"
    logger = setup_logging(logger_name=logger_name)
    initial_handler_count = len(logger.handlers)

    # Call it again
    logger_again = setup_logging(logger_name=logger_name)

    assert len(logger_again.handlers) == initial_handler_count
    assert logger is logger_again


def test_log_format_includes_thread_name():
    """Verify that the log format correctly includes the thread name as required by the spec."""
    logger = setup_logging(logger_name="thread_test_logger")

    # To capture output, we can temporarily add a stream handler
    log_stream = StringIO()
    stream_handler = logging.StreamHandler(log_stream)
    stream_handler.setFormatter(logger.handlers[0].formatter)
    logger.addHandler(stream_handler)

    custom_thread_name = "MyTestThread"
    log_message = "This is a test message."

    def log_in_thread():
        logger.info(log_message)

    thread = threading.Thread(target=log_in_thread, name=custom_thread_name)
    thread.start()
    thread.join()

    # Clean up the handler
    logger.removeHandler(stream_handler)

    log_output = log_stream.getvalue()
    assert f"[{custom_thread_name}]" in log_output
    assert log_message in log_output
