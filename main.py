import os

from preprocessing import Preprocess
from spider import catch_all_post, get_dcard_comment, get_dcard_post

if __name__ == "__main__":

    database_name = 'dcard.sqlite3'

    # if os.path.isfile(database_name):
    #     latest_dcard_post = get_dcard_post(limit=1)
    #     latest_dcard_post_number = latest_dcard_post['id'][0]
    # else:

    with Preprocess(database_name) as p:

        if os.path.isfile(database_name):
            posts = p.get_post()
        else:
            posts = catch_all_post()
            p.check_database()
            p.post_input(posts)

        for post_id in posts.id:
            comment_data = get_dcard_comment(post_id)
            p.comment_input(comment_data)
