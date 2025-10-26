import logging
import sys
from threading import RLock

from colorama import Fore, Style, init

# Perform initialization for colorama
init(autoreset=True)


class ColorFormatter(logging.Formatter):
    """A logging formatter that adds color to log levels."""

    LEVEL_COLORS = {
        logging.DEBUG: Fore.BLUE,
        logging.INFO: Fore.GREEN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.MAGENTA,
    }

    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno)
        level_name = f"{color}{record.levelname}{Style.RESET_ALL}" if color else record.levelname
        record.levelname = level_name
        return super().format(record)


# Thread-safe lock for logger initialization
_lock = RLock()
_logger_initialized = False


def setup_logging(level=logging.INFO):
    """
    Set up the root logger for the application.

    This function is thread-safe and ensures that logging is configured only once.
    """
    global _logger_initialized
    with _lock:
        if _logger_initialized:
            return

        logger = logging.getLogger("omopcloudetl_core")
        logger.setLevel(level)

        # Prevent log messages from being propagated to the root logger
        logger.propagate = False

        # Create a handler that writes to standard error
        handler = logging.StreamHandler(sys.stderr)

        # Create a formatter and set it for the handler
        formatter = ColorFormatter(
            "%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(handler)

        _logger_initialized = True
        logger.info("Logger initialized.")


# Automatically set up logging when the module is imported
setup_logging()

# You can get the logger by calling logging.getLogger("omopcloudetl_core")
