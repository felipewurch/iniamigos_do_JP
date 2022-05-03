import pandas as pd
from pandas import DataFrame
from typing import List
import time
from functools import reduce
import requests
import awswrangler as wr


def get_coins_list() -> List[dict]:
    return requests.get('https://api.coingecko.com/api/v3/coins/list').json()


def get_all_coins_details(coin_list: list, rate: int = 50) -> tuple:
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


def get_dict_info(coin_detail: dict, column_name: str) -> dict:
    try:
        dict_info: dict = coin_detail[f"{column_name}"]
    except:
        return {"id": [coin_detail["id"]]}
    dict_info["id"] = coin_detail["id"]
    final_dict: dict = {key: [value] for key, value in dict_info.items()}
    if '' in final_dict.keys(): final_dict.pop('')
    return final_dict


def get_dict_df(dict_info: dict) -> DataFrame:
    df: DataFrame = pd.DataFrame(dict_info)
    columns: List[str] = df.columns.to_list()
    columns.remove("id")
    ordered_columns: List[str] = ["id", *columns]
    return df[ordered_columns]


def concat_df(df1, df2) -> DataFrame:
    return pd.concat([df1, df2], ignore_index=True)


def get_dict_df_all(coin_detail_list: List[dict], column_name: str) -> DataFrame:
    dict_info_list = [get_dict_info(coin_detail, column_name) for coin_detail in coin_detail_list]
    dict_df_list = [get_dict_df(dict_info) for dict_info in dict_info_list]
    return reduce(concat_df, dict_df_list)


def save_dict_columns_dfs_on_athena(all_coins_details: tuple, columns_list: List[str]):
    for column in columns_list:
        df: DataFrame = get_dict_df_all(all_coins_details, column)
        df = df.astype(str)
        print(f"Saving on athena {column}.parquet")
        wr.s3.to_parquet(
            df=df,
            path=f"s3://mentoria-de-data-lake/coins/{column}.parquet",
            index=False,
            dataset=True,
            mode='overwrite',
            database="coins",
            table=f"{column}"
        )


def main():
    coins_list = get_coins_list()
    all_coins_details = get_all_coins_details(coins_list)
    all_coins_details_df = pd.DataFrame(all_coins_details)

    all_coins_details_df = all_coins_details_df.astype(str)

    wr.s3.to_parquet(
        df=all_coins_details_df,
        path="s3://mentoria-de-data-lake/coins/all_coins_details.parquet",
        index=False,
        dataset=True,
        mode='overwrite',
        database="coins",
        table="all_coins_details"
    )

    print("uploaded all_coins_details to athena")

    all_coins_details_dict_columns = [
        "platforms",
        "localization",
        "description",
        "links",
        "image",
        "market_data",
        "community_data",
        "developer_data",
        "public_interest_stats"
    ]

    save_dict_columns_dfs_on_athena(all_coins_details, all_coins_details_dict_columns)

    dict_market_data_all = get_dict_df_all(all_coins_details, "market_data")

    market_columns = [
        "current_price",
        "ath",
        "ath_change_percentage",
        "ath_date,"
        "atl",
        "price_change_percentage_30d_in_currency",
        "price_change_percentage_60d_in_currency",
        "price_change_percentage_200d_in_currency",
        "price_change_percentage_1y_in_currency",
        "market_cap_change_24h_in_currency",
        "market_cap_change_percentage_24h_in_currency"
    ]

    market_data = dict_market_data_all.to_dict(orient="records")

    save_dict_columns_dfs_on_athena(market_data, market_columns)


if __name__ == '__main__':
    main()
