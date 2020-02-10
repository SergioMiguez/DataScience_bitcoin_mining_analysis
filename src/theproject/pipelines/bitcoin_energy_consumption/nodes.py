
import pandas as pd
from functools import wraps
from typing import Callable
import time
import logging


def log_running_time(func: Callable) -> Callable:
    """Decorator for logging node execution time.

        Args:
            func: Function to be executed.
        Returns:
            Decorator for logging the running time.

    """

    @wraps(func)
    def with_time(*args, **kwargs):
        log = logging.getLogger(__name__)
        t_start = time.time()
        result = func(*args, **kwargs)
        t_end = time.time()
        elapsed = t_end - t_start
        log.info("Running %r took %.2f seconds", func.__name__, elapsed)
        return result

    return with_time


@log_running_time
def process_bitcoin_energy_consumption(energy_consumption: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for companies.

        Args:
            energy_consumption: Source data.
        Returns:
            Preprocessed data.

    """
    energy_consumption = energy_consumption.drop(["Timestamp", "MAX", "MIN"], axis=1) 

    return energy_consumption



'''
ourmatrix[].applyfunction
a = "day dffdsf dfdhf 2016"
y = a.split()
whatwewant = y[len(y)-1]
'''