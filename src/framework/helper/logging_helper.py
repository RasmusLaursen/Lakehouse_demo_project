import logging


def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a logger with the specified name.

    This function checks if a logger with the given name already exists.
    If it does not, it creates a new logger, sets its logging level to
    INFO, and adds a console handler that outputs log messages to the
    standard output. The log messages are formatted to include the
    timestamp, logger name, log level, and the actual log message.

    Args:
        name (str): The name of the logger to be created or retrieved.

    Returns:
        logging.Logger: A logger instance configured with the specified name.
    """
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        # Set logging level
        logger.setLevel(logging.INFO)

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
