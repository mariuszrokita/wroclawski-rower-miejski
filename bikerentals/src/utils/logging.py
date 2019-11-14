import logging
import pandas as pd
from functools import wraps


def get_logger():
    # Get the top-level logger object
    log = logging.getLogger()

    # make it print to the console.
    console = logging.StreamHandler()
    log.addHandler(console)

    # set logging level
    log.setLevel(logging.INFO)
    return log


# create an instance of a logger
logger = get_logger()


def log_transformation(stage='Unknown', indent_level=0):
    """
    Decorator used to log information about data transformation process.

    Parameters
    ----------
    stage : text
        Name of the data transformation stage.

    indent_level : int
        Indentation level used to format logged images.

    Returns
    -------
    func : obj
        Decorated function.
    """
    def logging_decorator(func):
        @wraps(func)
        def inner(*args, **kwargs):
            indent = indent_level * 4 * ' '

            # log ante factum
            logger.info(f"{indent}**** {stage} stage - start ****")
            for arg in [arg for arg in args if isinstance(arg, pd.DataFrame)]:
                logger.info(f"{indent}Input data shape: {arg.shape}")

            # execute decorated function
            result = func(*args, **kwargs)
            if isinstance(result, pd.DataFrame):
                logger.info(f"{indent}Output data shape: {result.shape}")

            # log post factum
            logger.info(f"{indent}**** {stage} stage - end ****")
            logger.info("")

            # return a result returned by decorated function
            return result
        return inner
    return logging_decorator
