"""ETF Pulse 공통 유틸 — 로깅, retry, 에러 핸들링"""
import sys
import time
import logging
from pathlib import Path
from functools import wraps

LOG_DIR = Path(__file__).parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)


def setup_logger(name='etf_pulse'):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    # File handler
    fh = logging.FileHandler(LOG_DIR / f'{name}.log', encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def retry(max_tries=3, delay=2, backoff=2, exceptions=(Exception,)):
    """retry decorator with exponential backoff"""
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            d = delay
            last_e = None
            for attempt in range(max_tries):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_e = e
                    if attempt < max_tries - 1:
                        time.sleep(d)
                        d *= backoff
                    else:
                        raise
        return wrapper
    return deco


def safe_call(fn, default=None, log=None):
    """예외 시 default 반환"""
    try:
        return fn()
    except Exception as e:
        if log:
            log.warning(f'{fn.__name__ if hasattr(fn, "__name__") else "call"} failed: {e}')
        return default
