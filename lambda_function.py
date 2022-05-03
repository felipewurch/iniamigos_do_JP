import json
import boto3
import requests
import pandas as pd
import awswrangler as wr

s3 = boto3.client('s3')


def __request(url):
    try:
        response = requests.get(url)
        content = json.loads(response.content.decode('utf-8'))
        return content
    except requests.exceptions.RequestException:
        raise ValueError(content)


def get_coins_market(base_url, vs_currency):
    """list all market data includes coins price,market cap,volume """
    """sample url : https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"""
    api_url = '{0}coins/markets?vs_currency={1}'.format(base_url, vs_currency)
    return __request(api_url)


def lambda_handler(event=None, context=None):
    base_url = "https://api.coingecko.com/api/v3/"
    cmkt_json = get_coins_market(base_url, 'usd')
    df = pd.DataFrame(cmkt_json)
    wr.s3.to_parquet(
        df=df,
        path="s3://mentoria-de-data-lake/market_update/market_data.parquet",
        index=False,
        dataset=True,
        mode='overwrite_partitions',
        partition_cols=["last_updated"],
        database="market_updates",
        table="market_data"
    )
