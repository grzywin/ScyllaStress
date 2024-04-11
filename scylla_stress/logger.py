"""
Custom logger which I am using in multiple personal projects
"""

import logging
import sys
import os
from datetime import datetime

# Define custom log level NOTE
NOTE = 25
logging.addLevelName(NOTE, "NOTE")

# Define color codes for different log levels
COLORS = {
    logging.DEBUG: '\033[94m',  # Blue
    logging.INFO: '\033[92m',  # Green
    logging.WARNING: '\033[93m',  # Yellow
    logging.ERROR: '\033[91;1m',  # Red
    logging.CRITICAL: '\033[95m',  # Purple
    NOTE: '\033[96m'  # Cyan for NOTE level
}
RESET = '\033[0m'  # Reset color


class ColoredFormatter(logging.Formatter):
    """
    Custom Formatter for adding color to console output
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s',
                                                   datefmt='%H:%M:%S %Y-%m-%d')

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log
        :param record: LogRecord object
        """
        log_level = record.levelno
        color = COLORS.get(log_level, '\033[0m')  # Default to no color

        # Get the default log message using the specified format
        default_log_message = self.default_formatter.format(record)

        log_format = f"{color}{default_log_message}{RESET}"
        return log_format


# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Console handler with colored output
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = ColoredFormatter()
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# File handler
relative_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
log_file_path = os.path.join(relative_path, "logs", f"scylla_stress_{datetime.now().strftime('%H_%M_%S_%y_%m_%d')}.log")
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(asctime)s] %(message)s',
                                   datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)


# Add NOTE log level method to logger
# pylint: disable=protected-access
def note(self, message: str, *args, **kwargs):
    """
    Added note log level
    :param self: Log object instance
    :param message: Content of message
    """
    if self.isEnabledFor(NOTE):
        self._log(NOTE, message, args, **kwargs)


logging.Logger.note = note
