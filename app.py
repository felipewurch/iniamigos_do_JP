import pandas as pd
import logging
import time
import json
import requests
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects, RetryError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logging.basicConfig(level=logging.DEBUG)


def retrieve_coin_details(id: str) -> dict:
    response = requests.get(f'https://api.coingecko.com/api/v3/coins/{id}')
    logging.debug(response.status_code)
    if response.status_code == 200:
        return response.json()
    time.sleep(1000)
    return retrieve_coin_details(id)

coingecko_coins_list_response = requests.get('https://api.coingecko.com/api/v3/coins/list').json()

coin_details_list = [retrieve_coin_details(item.get("id")) for item in coingecko_coins_list_response[:50]]

coin_details_df = pd.DataFrame(coin_details_list)
