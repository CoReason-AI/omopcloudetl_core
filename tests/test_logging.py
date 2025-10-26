import logging
import unittest
from unittest.mock import patch, MagicMock

from colorama import Fore, Style

from omopcloudetl_core.logging import ColorFormatter, setup_logging


class TestLogging(unittest.TestCase):
    @patch("omopcloudetl_core.logging.logging.getLogger")
    def test_setup_logging_initialization(self, mock_get_logger):
        """Test that the logger is initialized only once."""
        # Reset the initialization flag for testing purposes
        from omopcloudetl_core import logging as custom_logging

        custom_logging._logger_initialized = False

        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # First call to setup_logging should initialize the logger
        setup_logging()
        self.assertTrue(custom_logging._logger_initialized)
        mock_get_logger.assert_called_with("omopcloudetl_core")
        mock_logger.setLevel.assert_called_with(logging.INFO)
        self.assertEqual(mock_logger.addHandler.call_count, 1)

        # Subsequent calls should not re-initialize the logger
        setup_logging()
        self.assertEqual(mock_logger.addHandler.call_count, 1)

    def test_color_formatter(self):
        """Test that the ColorFormatter adds color codes to log levels."""
        # Use a format string that includes the level name
        formatter = ColorFormatter("%(levelname)s - %(message)s")
        record = logging.LogRecord("test", logging.INFO, "/path/to/test", 1, "Test message", (), None)

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


if __name__ == "__main__":
    unittest.main()
