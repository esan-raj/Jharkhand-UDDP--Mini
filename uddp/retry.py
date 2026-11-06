import logging
import time
from functools import wraps

logger = logging.getLogger("uddp")


def with_retry(times, backoff_seconds, exceptions=(Exception,)):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = backoff_seconds
            for attempt in range(1, times + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == times:
                        raise
                    logger.warning(
                        "Attempt %s/%s failed for %s: %s",
                        attempt,
                        times,
                        func.__name__,
                        exc,
                    )
                    time.sleep(delay)
                    delay *= 2

        return wrapper

    return decorator
