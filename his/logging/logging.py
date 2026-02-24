import logging
import inspect


def _init_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
        ],
    )
    logging.getLogger("strands").setLevel(logging.INFO)

    caller_name = inspect.currentframe().f_back.f_globals['__name__']

    logger = logging.getLogger(caller_name)
    logger.setLevel(logging.INFO)

    return logger