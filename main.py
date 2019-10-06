from preprocessing import Preprocess
from spider import catch_all_data, output_to_csv, get_dcard_comment, get_dcard_post

if __name__ == "__main__":
    # a, b = catch_all_data()
    post_id = 232227969
    # a = get_dcard_post(post_id)
    b = get_dcard_comment(post_id)
    # output_to_csv(a, b)

    with Preprocess('dcard.sqlite3') as p:
        p.check_database()
        # p.post_input(a)
        p.comment_input(b)
