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
import unittest

from colorama import Fore, Style

from omopcloudetl_core.logging import ColorizedFormatter, get_logger


class TestLogging(unittest.TestCase):
    def test_get_logger(self):
        """Test that get_logger returns a configured logger."""
        logger = get_logger("test_logger")
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.level, logging.INFO)
        self.assertEqual(len(logger.handlers), 1)
        self.assertIsInstance(logger.handlers[0].formatter, ColorizedFormatter)

    def test_colorized_formatter(self):
        """Test that the ColorizedFormatter adds color codes to log levels."""
        formatter = ColorizedFormatter("%(levelname)s - %(message)s")
        record = logging.LogRecord("test", logging.INFO, "/path/to/test", 1, "Test message", (), None)
        record.levelname = "INFO"
        # The levelname should be wrapped in color codes
        formatted_message = formatter.format(record)

        # Construct the expected colored level name
        expected_info = f"{Fore.GREEN}INFO{Style.RESET_ALL}"
        self.assertIn(expected_info, formatted_message)
        self.assertIn("Test message", formatted_message)

        # Test another level
        record.levelno = logging.ERROR
        record.levelname = "ERROR"
        formatted_message = formatter.format(record)
        expected_error = f"{Fore.RED}ERROR{Style.RESET_ALL}"
        self.assertIn(expected_error, formatted_message)
        self.assertIn("Test message", formatted_message)

    def test_get_logger_singleton(self):
        """Test that get_logger returns the same instance for the same name."""
        logger1 = get_logger("singleton_logger")
        logger2 = get_logger("singleton_logger")
        self.assertIs(logger1, logger2)


if __name__ == "__main__":
    unittest.main()
