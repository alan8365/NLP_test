import requests
import pandas as pd
import os
import sys
import datetime
import urllib3
import time

from uuid import UUID
from pandas import read_json, DataFrame, to_datetime

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
    while not json_content:
        time.sleep(1)
        json_content = requests.get(url, verify=False, headers=headers).content

    df = read_json(json_content)
    columns = ['id', 'title', 'excerpt', 'updatedAt', 'commentCount', 'likeCount', 'topics']
    df = df[columns]

    df.updatedAt = to_datetime(df.updatedAt)

    df = df.rename(columns={'excerpt': 'content',
                            'updatedAt': 'updated_at',
                            'commentCount': 'comment_count',
                            'likeCount': 'like_count'})

    return df


def get_dcard_comment(post_id: int) -> DataFrame:
    url = f"https://www.dcard.tw/_api/posts/{post_id}/comments?limit=100"
    json_content = requests.get(url, verify=False, headers=headers).content

    # 防止反爬傳空值
    while not json_content:
        time.sleep(1)
        json_content = requests.get(url, verify=False, headers=headers).content

    if json_content == b'[]':
        return DataFrame()

    df = read_json(json_content)
    # 把已經刪除的內容去掉
    df = df[~df['hidden']]

    if df.empty:
        return df

    columns = ['id', 'postId', 'updatedAt', 'floor', 'content', 'likeCount']
    df.updatedAt = to_datetime(df.updatedAt)
    return df[columns].rename(columns={'postId': 'post_id',
                                       'updatedAt': 'updated_at',
                                       'likeCount': 'like_count'})


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
    path = fr'{os.getcwd()}\raw_data'
    timestamp = datetime.datetime.now().strftime('%Y%m%d')
    filename = fr'{path}\dcard_nutc_post_{timestamp}.json'
    all_post.to_json(filename)
    filename = fr'{path}\dcard_nutc_comment_{timestamp}.json'
    all_comment.to_json(filename)


def output_to_csv(all_post: DataFrame, all_comment: DataFrame) -> None:
    path = fr'{os.getcwd()}\raw_data'
    timestamp = datetime.datetime.now().strftime('%Y%m%d')
    filename = fr'{path}\dcard_nutc_post_{timestamp}.csv'
    all_post.to_csv(filename)
    filename = fr'{path}\dcard_nutc_comment_{timestamp}.csv'
    all_comment.to_csv(filename)
