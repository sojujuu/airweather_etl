import logging, os, datetime
from .config import Paths

def get_logger(name="airweather.etl"):
    os.makedirs(Paths.LOG_DIR, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    logfile = os.path.join(Paths.LOG_DIR, f"LOG_{ts}.log")
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    # avoid duplicate handlers during tests
    if not logger.handlers:
        fh = logging.FileHandler(logfile, encoding="utf-8")
        ch = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        fh.setFormatter(fmt); ch.setFormatter(fmt)
        logger.addHandler(fh); logger.addHandler(ch)
    logger.info("Log file initialized.")
    return logger
