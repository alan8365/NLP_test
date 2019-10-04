import requests
import pandas as pd
import os
import sys
import datetime
import urllib3
import time

from uuid import UUID
from pandas import read_json, DataFrame

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0"
}

urllib3.disable_warnings()


def get_dcard_post(post_id: int = None) -> DataFrame:
    if post_id:
        url = f"https://www.dcard.tw/_api/forums/nutc/posts?popular=false&limit=100&before={post_id}"
    else:
        url = f"https://www.dcard.tw/_api/forums/nutc/posts?popular=false&limit=100"

    json_content = requests.get(url, verify=False, headers=headers).content
    df = read_json(json_content)
    columns = ['id', 'title', 'excerpt', 'updatedAt', 'commentCount', 'likeCount', 'topics']
    return df[columns]


def get_dcard_comment(post_id: int) -> DataFrame:
    url = f"https://www.dcard.tw/_api/posts/{post_id}/comments"
    json_content = requests.get(url, verify=False, headers=headers).content
    if json_content:
        df = read_json(json_content)
        columns = ['id', 'postId', 'updatedAt', 'floor', 'content', 'likeCount']
        return df[columns]
    else:
        return DataFrame()


def catch_all_data():
    all_post = DataFrame()
    all_comment = DataFrame()

    df = get_dcard_post()
    while True:
        all_post = all_post.append(df)

        if len(df) == 100:
            last_id = df['id'][99]
            df = get_dcard_post(last_id)
            print(last_id)
        else:
            break

    for post_id in all_post['id']:
        df = get_dcard_comment(post_id)
        all_comment = all_comment.append(df)

    return all_post, all_comment


def output_to_json(all_post: DataFrame, all_comment: DataFrame) -> None:
    timestamp = datetime.datetime.now().strftime('%Y%m%d')
    filename = f'raw/dcard_nutc_post_{timestamp}.json'
    all_post.to_json(filename)
    filename = f'raw/dcard_nutc_comment_{timestamp}.json'
    all_comment.to_json(filename)
