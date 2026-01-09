# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

import logging

# ---------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------

def get_class_logger(cls: str, verbosity: int) -> logging.Logger:

    logger_name = f"{cls.__module__}.{cls.__name__}"
    logger = logging.getLogger(logger_name)
    
    if verbosity <= 0:
        logger.setLevel(logging.ERROR)
    elif verbosity == 1:
        logger.setLevel(logging.WARNING)
    elif verbosity == 2:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)
    
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False
    
    return logger