import pandas as pd
from functools import wraps
from typing import Callable
import time
import logging
import math
import matplotlib.pyplot as plt


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
'''


def stringtofloat(x):
    return  int(float(x))
def parse_comma(x):
    return (x.replace(",",""))
def floattoint(x):
    return (math.floor(x))

def modifyStringto(reversed_cryptocurrencies:pd.DataFrame) -> pd.DataFrame:
    new_cryptocurrecncies = pd.DataFrame({"Value":[]})
    new_cryptocurrecncies["Value"] = reversed_cryptocurrencies["Close"].apply(parse_comma).apply(stringtofloat)
    return new_cryptocurrecncies


def modifyStringtofloat(formated_total_bitcoins:pd.DataFrame)->pd.DataFrame:
    new_bitcoins = pd.DataFrame({"Value":[]})
    new_bitcoins["Value"] = formated_total_bitcoins["value"].apply(stringtofloat)
    return new_bitcoins

    
def modify_process_energy(preprocess_energy:pd.DataFrame) -> pd.DataFrame:
    new_process_energy = pd.DataFrame({"Value":[]})
    new_process_energy["Value"] = preprocess_energy["Price"].apply(floattoint)
    return new_process_energy



@log_running_time
def calculate_profit(new_cryptocurrencies: pd.DataFrame, new_total_bitcoins:pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for currancies.

        Filtering Bitcoins
    
            preprocess_cryptocurrencies: Source data.
        Returns:
            Preprocessed data.
        

    """
    a = pd.DataFrame({"Value":[]})
    
    a["Value"] = new_cryptocurrencies["Value"].mul(new_total_bitcoins["Value"])
    return a


def calculate_substraction(profit:pd.DataFrame, new_process_energy: pd.DataFrame, cryptocurrency_reverse: pd.DataFrame) -> pd.DataFrame:
    a = pd.DataFrame({"Value":[],"Date":[]})
    a["Value" ] = profit["Value"] - new_process_energy["Value"]
    a["Date"] = cryptocurrency_reverse["Date"]
    a.drop(a.tail(14).index, inplace = True)

    return a

def graph(benefit:pd.DataFrame):

    plt.plot(benefit["Date"], benefit["Value"])
    plt.savefig('results/myfig_benefit')
def co2_emissions(new_process_energy:pd.DataFrame, cryptocurrency_reverse: pd.DataFrame)->pd.DataFrame:
    a = pd.DataFrame({"Value":[]})

    a["Value"] = new_process_energy["Value"].apply(lambda x: x*(0.9884*10**9))
    a["Date"] = cryptocurrency_reverse["Date"]
    return a
def graph2(co2_emission:pd.DataFrame):

    plt.plot(co2_emission["Date"], co2_emission["Value"])
    plt.savefig('results/myfig_emissions')


   