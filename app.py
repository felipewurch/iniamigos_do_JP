import pandas as pd
from pandas import DataFrame
from typing import List, Dict
import logging
import time
from functools import reduce
import requests
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects, RetryError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import awswrangler as wr


def get_coins_list() -> List[dict]:
    return requests.get('https://api.coingecko.com/api/v3/coins/list').json()


def get_all_coins_details(coin_list: list, rate: int = 50) -> list:
    out_list = []
    request_pool = [0]
    while (len(coin_list) > 0):
        current_id = coin_list[0]["id"]
        request_pool.append(time.perf_counter())
        response = requests.get(f'https://api.coingecko.com/api/v3/coins/{current_id}')
        if len(request_pool) > rate:
            request_pool.pop(0)
        if response.status_code == 200:
            coin_list.pop(0)
            out_list.append(response.json())
            print(f"{current_id} ok, {len(coin_list)} more to go")
        else:
            requests_time_span = request_pool[-1] - request_pool[0]
            print(f"{requests_time_span + 1}\n{request_pool}")
            if (requests_time_span + 1) < rate:
                time.sleep(int(requests_time_span))
    return tuple(out_list)


def main():
    coin_list = get_coins_list()
    coin_details = get_all_coins_details(coin_list)
    df = DataFrame(coin_details)
    coin_list_df = DataFrame(coin_list)
    coin_list_df.to_csv("coin_list.csv", index=False)
    df.to_csv("coin_details.csv", index=False)


if __name__ == '__main__':
    main()
