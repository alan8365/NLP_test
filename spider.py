import requests
# from bs4 import BeautifulSoup
import pandas as pd
# import numpy as np
import os
import sys
# import re
import datetime
import urllib3
# import csv
# import urllib
import time
# import json
# import cfscrape

urllib3.disable_warnings()
try:
    allpost = []
    allcom = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36"}

    counter = 0
    while counter < 100:
        post_contents = []
        comment_contents = []
        list_of_post = []
        this_url = ''

        if counter == 0:
            this_url = f"https://www.dcard.tw/_api/forums/nutc/posts?popular=false&limit=100&before=229137137"
            r = requests.get(this_url, verify=False, headers=headers)
            if r.content:
                list_of_post = r.json()
                last_id = list_of_post[99]['id']
                print(this_url)
                print(r.status_code)
        elif counter > 0:
            this_url = f"https://www.dcard.tw/_api/forums/nutc/posts?popular=false&limit=100&before={last_id}"
            r = requests.get(this_url, verify=False, headers=headers)
            if r.content:
                list_of_post = r.json()
                last_id = list_of_post[99]['id']
                print(this_url)
                print(r.status_code)

        time.sleep(1)
        # get every post's id
        for post in list_of_post:
            ids = str(post['id'])
            print(ids)
            # get post
            post = requests.get(f'http://dcard.tw/_api/posts/{ids}', verify=False, headers=headers)
            if post.content:
                post_json = post.json()
                # 文章id
                post_id = post_json['id']
                # 文章標題
                post_title = post_json['title']
                # 文章內文
                post_content = post_json['content']
                # 發文時間
                post_createdAt = post_json['createdAt']
                # 更新時間
                post_updatedAt = post_json['updatedAt']
                # 留言數
                post_commentCount = post_json['commentCount']
                # 愛心數
                post_likeCount = post_json['likeCount']
                # 話題
                post_topics = post_json['topics']
                # 塞入陣列
                p = [post_id, post_title, post_content, post_createdAt, post_updatedAt, post_commentCount,
                     post_likeCount, post_topics]
                # print(p)
                post_contents.append(p)

            time.sleep(2)
            # get post's comment
            comment = requests.get(f'https://www.dcard.tw/_api/posts/{ids}/comments', verify=False, headers=headers)
            if comment.content:
                # comment = re.sub(r'^jsonp\d+\(|\)\s+$', '', comment.text)
                # comment.encoding = 'utf-8'
                # comment_json = json.loads(comment)
                comment_json = comment.json()
                for com_json in comment_json:
                    if com_json['hiddenByAuthor'] == False | com_json['hidden'] == False:
                        # 回覆的文章id
                        comment_post_id = com_json['postId']
                        # 樓層
                        comment_floor = com_json['floor']
                        # 回覆內容
                        comm_content = com_json['content']
                        # 回覆時間
                        comment_createdAt = com_json['createdAt']
                        # 更新時間
                        comment_updatedAt = com_json['updatedAt']
                        # 留言愛心數
                        comment_likeCount = com_json['likeCount']
                        # 塞入陣列
                        c = [comment_post_id, comment_floor, comm_content, comment_createdAt, comment_updatedAt,
                             comment_likeCount]
                        # print(c)
                        comment_contents.append(c)

        allpost.extend(post_contents)
        allcom.extend(comment_contents)
        time.sleep(3)
        counter = counter + 1
        print(counter)

    # 轉成dataframe 為了最後輸出成csv
    df = pd.DataFrame(allpost, columns=["id", "title", "content", "createdAt", "updatedAt", "commentCount", "likeCount",
                                        "topics"])
    df.head()
    cdf = pd.DataFrame(allcom, columns=["postId", "floor", "content", "createdAt", "updatedAt", "likeCount"])
    cdf.head()

    cwd = os.getcwd()
    timestamp = datetime.datetime.now()
    timestamp = timestamp.strftime('%Y%m%d')
    post_filename = os.path.join(cwd, 'dcard_nutc_post_{}_02.csv'.format(timestamp))
    df.to_csv(post_filename, index=False, encoding='utf_8_sig')
    print('Save csv to {}'.format(post_filename))

    comment_filename = os.path.join(cwd, 'dcard_nutc_comment_{}_02.csv'.format(timestamp))
    cdf.to_csv(comment_filename, index=False, encoding='utf_8_sig')
    print('Save csv to {}'.format(comment_filename))

except:
    # 轉成dataframe 為了最後輸出成csv
    df = pd.DataFrame(allpost, columns=["id", "title", "content", "createdAt", "updatedAt", "commentCount", "likeCount",
                                        "topics"])
    df.head()
    cdf = pd.DataFrame(allcom, columns=["postId", "floor", "content", "createdAt", "updatedAt", "likeCount"])
    cdf.head()

    cwd = os.getcwd()
    timestamp = datetime.datetime.now()
    timestamp = timestamp.strftime('%Y%m%d')
    post_filename = os.path.join(cwd, 'dcard_nutc_post_{}_02.csv'.format(timestamp))
    df.to_csv(post_filename, index=False, encoding='utf_8_sig')
    print('Save csv to {}'.format(post_filename))

    comment_filename = os.path.join(cwd, 'dcard_nutc_comment_{}_02.csv'.format(timestamp))
    cdf.to_csv(comment_filename, index=False, encoding='utf_8_sig')
    print('Save csv to {}'.format(comment_filename))

    print("Unexpected error:", sys.exc_info())
    print(post)
