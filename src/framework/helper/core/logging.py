"""Core logging utilities for framework."""
import logging


def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a logger with the specified name.

    This function creates or retrieves a logger with the specified name,
    sets its logging level to INFO, and ensures a console handler is configured
    to output log messages to standard output. The log messages include the
    timestamp, logger name, log level, and the actual log message.

    Args:
        name (str): The name of the logger to be created or retrieved.

    Returns:
        logging.Logger: A logger instance configured with the specified name.
    """
    logger = logging.getLogger(name)
    
    # Always set the logger level to INFO to ensure INFO messages are captured
    logger.setLevel(logging.INFO)
    
    # Only add console handler if it doesn't already exist
    if not any(isinstance(handler, logging.StreamHandler) for handler in logger.handlers):
        # Create a console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Create a formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(console_handler)

    return logger
