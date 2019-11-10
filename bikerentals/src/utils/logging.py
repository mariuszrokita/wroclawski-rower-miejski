import logging


def get_logger():
    # Get the top-level logger object
    log = logging.getLogger()

    # make it print to the console.
    console = logging.StreamHandler()
    log.addHandler(console)

    # set logging level
    log.setLevel(logging.INFO)
    return log


logger = get_logger()
