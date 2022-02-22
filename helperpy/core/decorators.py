from typing import Callable
import functools
import random
import time

from helperpy.core.utils import get_timetaken_fstring


def timer(func: Callable) -> Callable:
    """Decorator that prints the runtime of the decorated function"""
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        time_taken_in_secs = round(end - start, 3)
        timetaken_fstring = get_timetaken_fstring(num_seconds=time_taken_in_secs)
        print(f"Executed {func.__name__!r} in: {timetaken_fstring}")
        return result
    return wrapper_timer


def slow_down(func: Callable) -> Callable:
    """
    Decorator that slows down the execution of the decorated function.
    Slows down the execution by 10-20 minutes.
    """
    @functools.wraps(func)
    def wrapper_slow_down(*args, **kwargs):
        num_seconds_delayed = random.randint(600, 1200)
        time.sleep(num_seconds_delayed)
        return func(*args, **kwargs)
    return wrapper_slow_down


def repeat(num_times: int) -> Callable:
    """Decorator that executes the decorated function `num_times` times"""
    def repeat_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for _ in range(num_times):
                result = func(*args, **kwargs)
            return result
        return wrapper
    return repeat_decorator