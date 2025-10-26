import sys
from loguru import logger

# Remove the default logger
logger.remove()

# Add a new logger with a custom format that includes the thread name
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
           "<level>{level: <8}</level> | "
           "<cyan>{thread.name: <15}</cyan> | "
           "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
           "<level>{message}</level>",
    colorize=True,
    enqueue=True,  # Make it thread-safe
    backtrace=True,
    diagnose=True
)

# Export the configured logger
__all__ = ["logger"]
