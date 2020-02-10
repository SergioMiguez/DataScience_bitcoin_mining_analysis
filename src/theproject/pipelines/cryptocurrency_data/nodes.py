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


#def _is_true(x):
 #   return x == "t"

def _is_Bitcoin(x):
    return x == "bitcoin"



'''
def _parse_percentage(x):
    if isinstance(x, str):
        return float(x.replace("%", "")) / 100
    return float("NaN")
def _parse_money(x):
    return float(x.replace("$", "").replace(",", ""))
'''


@log_running_time
def preprocess_cryptocurrencies(currencies: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for currancies.

        Preparing data base to filter Bitcoins
    
        Args:
            currancies: Source data.
        Returns:
            Preprocessed data.

    """

    currencies["Currency"] = currencies["Currency"].apply(_is_Bitcoin)

    return currencies

@log_running_time
def filter_cryptocurrencies(preprocess_cryptocurrencies: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for currancies.

        Filtering Bitcoins
    
        Args:
            preprocess_cryptocurrencies: Source data.
        Returns:
            Preprocessed data.

    """

    preprocess_cryptocurrencies = preprocess_cryptocurrencies.query('Currency == True') 

    return preprocess_cryptocurrencies
def format(x):
    a =x.split()
    if  (int (a[len(a)-1]) < 2015):
        return (69)
    else:
        return (int (a[len(a)-1]))
         

def format_cryptocurrencies(filtered_cryptocurrencies:pd.DataFrame) ->pd.DataFrame:
    filtered_cryptocurrencies["Date"] = filtered_cryptocurrencies["Date"].apply(format)
    return filtered_cryptocurrencies

def yearvalid_cryptocurrencies(formated_cryptocurrencies:pd.DataFrame) ->pd.DataFrame:
    formated_cryptocurrencies = formated_cryptocurrencies.query('Date != 69')
    return formated_cryptocurrencies
def inverse_cryptocurrencies(yearvalids_cryptocurrencies:pd.DataFrame) ->pd.DataFrame:
    data = yearvalids_cryptocurrencies
    return data.reindex(index=data.index[::-1])
def formating(x):
    a1 = x.split("/")
    a2 = a1[len(a1)-1].split()
    return a2[0]
def format_totalbitcoins(preprocessed_totalbitcoins:pd.DataFrame) ->pd.DataFrame:
    preprocessed_totalbitcoins["Date"] = preprocessed_totalbitcoins["Date"].apply(formating)
    return preprocessed_totalbitcoins

'''
@log_running_time
def preprocess_shuttles(shuttles: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for shuttles.

        Args:
            shuttles: Source data.
        Returns:
            Preprocessed data.

    """
    shuttles["d_check_complete"] = shuttles["d_check_complete"].apply(_is_true)

    shuttles["moon_clearance_complete"] = shuttles["moon_clearance_complete"].apply(
        _is_true
    )

    shuttles["price"] = shuttles["price"].apply(_parse_money)

    return shuttles
'''

'''
def create_master_table(
    shuttles: pd.DataFrame, companies: pd.DataFrame, reviews: pd.DataFrame) -> pd.DataFrame:
    """Combines all data to create a master table.

        Args:
            shuttles: Preprocessed data for shuttles.
            companies: Preprocessed data for companies.
            reviews: Source data for reviews.
        Returns:
            Master table.

    """
    rated_shuttles = shuttles.merge(reviews, left_on="id", right_on="shuttle_id")

    with_companies = rated_shuttles.merge(
        companies, left_on="company_id", right_on="id"
    )

    master_table = with_companies.drop(["shuttle_id", "company_id"], axis=1)
    master_table = master_table.dropna()
    return master_table
'''