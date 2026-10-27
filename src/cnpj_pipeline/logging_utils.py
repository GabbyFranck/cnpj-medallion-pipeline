import logging

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Return a module logger.

    Inside Airflow the root logger is already configured and task logs are captured,
    so we only attach a handler when running standalone (scripts, tests).
    """
    logger = logging.getLogger(name)
    if not logging.getLogger().handlers and not logger.handlers:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    return logger
