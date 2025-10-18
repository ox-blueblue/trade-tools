"""
Trading logger with structured output and error handling.
"""

import os
import csv
import logging
from datetime import datetime
import pytz
from decimal import Decimal


class TradingLogger:
    """Enhanced logging with structured output and error handling."""

    def __init__(self, exchange: str, ticker: str, log_to_console: bool = False):
        self.exchange = exchange
        self.ticker = ticker
        # Ensure logs directory exists at the project root
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        logs_dir = os.path.join(project_root, 'logs')
        os.makedirs(logs_dir, exist_ok=True)

        order_file_name = f"{exchange}_{ticker}_orders.csv"
        debug_log_file_name = f"{exchange}_{ticker}_activity.log"

        account_name = os.getenv('ACCOUNT_NAME')
        if account_name:
            order_file_name = f"{exchange}_{ticker}_{account_name}_orders.csv"
            debug_log_file_name = f"{exchange}_{ticker}_{account_name}_activity.log"

        # Log file paths inside logs directory
        self.log_file = os.path.join(logs_dir, order_file_name)
        self.debug_log_file = os.path.join(logs_dir, debug_log_file_name)
        self.timezone = pytz.timezone(os.getenv('TIMEZONE', 'Asia/Shanghai'))

        log_to_console = os.getenv('LOG_TO_CONSOLE', 'false').lower() == 'true'
        log_to_file = os.getenv('LOG_TO_FILE', 'false').lower() == 'true'
        log_console_level = os.getenv('LOG_CONSOLE_LEVEL', 'INFO')
        log_file_level = os.getenv('LOG_FILE_LEVEL', 'INFO')
        self.logger = self._setup_logger(log_to_console, log_console_level, log_to_file, log_file_level)
        self.logger.debug(f"set log level to {log_console_level} for console and {log_file_level} for file")

    def _setup_logger(self, log_to_console: bool, log_console_level: str, log_to_file: bool, log_file_level: str) -> logging.Logger:
        """Setup the logger with proper configuration."""
        logger = logging.getLogger(f"trading_bot_{self.exchange}_{self.ticker}")
        logger.setLevel(logging.DEBUG)

        # Prevent propagation to root logger to avoid duplicate messages
        logger.propagate = False

        # Prevent duplicate handlers
        if logger.handlers:
            return logger

        class TimeZoneFormatter(logging.Formatter):
            def __init__(self, fmt=None, datefmt=None, tz=None):
                super().__init__(fmt=fmt, datefmt=datefmt)
                self.tz = tz

            def formatTime(self, record, datefmt=None):
                dt = datetime.fromtimestamp(record.created, tz=self.tz)
                if datefmt:
                    return dt.strftime(datefmt)
                return dt.isoformat()

        formatter = TimeZoneFormatter(
            "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            tz=self.timezone
        )

        # File handler if requested
        if log_to_file:
            file_handler = logging.FileHandler(self.debug_log_file)
            file_handler.setLevel(self.convert_log_level(log_file_level))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        # Console handler if requested
        if log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self.convert_log_level(log_console_level))
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    def log(self, message: str, level: str = "INFO"):
        """Log a message with the specified level."""
        formatted_message = f"[{self.exchange.upper()}_{self.ticker.upper()}] {message}"
        if level.upper() == "DEBUG":
            self.logger.debug(formatted_message)
        elif level.upper() == "INFO":
            self.logger.info(formatted_message)
        elif level.upper() == "WARNING":
            self.logger.warning(formatted_message)
        elif level.upper() == "ERROR":
            self.logger.error(formatted_message)
        else:
            self.logger.info(formatted_message)

    def convert_log_level(self, level: str) -> int:
        """Convert log level string to logging level integer."""
        level = level.upper()
        if level == "DEBUG":
            return logging.DEBUG
        elif level == "INFO":
            return logging.INFO
        elif level == "WARNING":
            return logging.WARNING
        elif level == "ERROR":
            return logging.ERROR
        else:
            return logging.INFO

    def log_transaction(self, order_id: str, side: str, quantity: Decimal, price: Decimal, status: str):
        """Log a transaction to CSV file."""
        try:
            timestamp = datetime.now(self.timezone).strftime("%Y-%m-%d %H:%M:%S")
            row = [timestamp, order_id, side, quantity, price, status]

            # Check if file exists to write headers
            file_exists = os.path.isfile(self.log_file)

            with open(self.log_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(['Timestamp', 'OrderID', 'Side', 'Quantity', 'Price', 'Status'])
                writer.writerow(row)

        except Exception as e:
            self.log(f"Failed to log transaction: {e}", "ERROR")
