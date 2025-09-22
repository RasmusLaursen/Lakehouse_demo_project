import logging


def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a logger with the specified name.
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
