import logging
import sys
import threading

from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)


class ColorizedFormatter(logging.Formatter):
    """
    A custom logging formatter that adds color to log levels.
    """

    LOG_LEVEL_COLORS = {
        logging.DEBUG: Fore.CYAN,
        logging.INFO: Fore.GREEN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.MAGENTA,
    }

    def format(self, record):
        color = self.LOG_LEVEL_COLORS.get(record.levelno)
        # Add color to levelname
        record.levelname = f"{color}{record.levelname}{Style.RESET_ALL}"
        return super().format(record)


_loggers: dict[str, logging.Logger] = {}
_lock = threading.Lock()


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a configured, thread-safe logger instance.

    This function ensures that a logger is configured only once and is safe
    to be called from multiple threads.
    """
    with _lock:
        if name in _loggers:
            return _loggers[name]

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = False  # Prevent logs from being passed to the root logger

        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = ColorizedFormatter("%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        _loggers[name] = logger
        return logger
