import re
from csv import DictReader
from sqlite3 import connect
from typing import Tuple, Iterable, Generator, List
from pandas import DataFrame


def extract_image(content: str) -> Tuple[str, list]:
    pattern = r"https:\/\/i\.imgur\.com\/.*.jpg"
    imgur_compile = re.compile(pattern)
    images = imgur_compile.findall(content)
    result = imgur_compile.sub("", content)

    return result, images


def detag(tittle: str) -> Tuple[str, List[str]]:
    pattern = r'#\w*'
    detag_compile = re.compile(pattern)
    tags = list(map(lambda s: s[1:], detag_compile.findall(tittle)))
    result = detag_compile.sub("", tittle).strip()

    if not result:
        result = " ".join(tags)

    return result, tags


def dict_filter(it: Iterable, *keys: str) -> Generator:
    for d in it:
        yield dict((k, d[k]) for k in d.keys() if k not in keys)


def dict_filter_to_tuple(it: Iterable, *keys: str) -> Generator:
    for d in it:
        yield tuple(d[k] for k in d.keys() if k not in keys)


class Preprocess:

    def __init__(self, database_name: str):
        self.database_name = database_name

    def __enter__(self):
        self.con = connect(self.database_name)
        self.cur = self.con.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()

    def check_table_exist(self, table_name: str) -> bool:
        sql = f"""
            select name
            from sqlite_master
            where type='table' and name = '{table_name}'
        """

        self.cur.execute(sql)
        return bool(self.cur.fetchall())

    def check_database(self) -> None:
        sql_dict = {
            "post": """
                create table post
                (
                    id int not null
                        constraint post_pk
                            primary key,
                    title text not null,
                    content text not null,
                    updated_at datetime not null,
                    comment_count int not null,
                    like_count int not null
                );
            """,
            "img_url": """
                create table img_url
                (
                    url text not null,
                    post_id int
                        constraint img_url_post_id_fk
                            references post
                                on update cascade on delete cascade
                );
            """,
            "post_tag": """
                create table post_tag
                (
                    post_id int not null
                        constraint post_tag_post_id_fk
                            references post
                                on update cascade on delete cascade,
                    tag_content text not null
                );
            """,
            "comment": """
                create table comment
                (
                    id integer
                        constraint comment_pk
                            primary key,
                    post_id int not null
                        references post
                            on update cascade on delete cascade,
                    update_at datetime not null ,
                    floor int not null,
                    content text not null,
                    like_count int not null
                );
            """,
            "response_comment": """
                create table response_comment
                (
                    comment_id int not null
                        constraint response_comment_comment_id_fk
                            references comment
                                on update cascade on delete cascade,
                    response_comment_id int
                        constraint response_comment_comment_id_fk_2
                            references comment
                                on update cascade on delete cascade
                );
            """,
            "top_response": """
                create table top_response
                (
                    post_id int
                        constraint top_response_post_id_fk
                            references post
                                on update cascade on delete cascade,
                    comment_id int
                        constraint top_response_comment_id_fk
                            references comment
                                on update cascade on delete cascade
                );
            """
        }

        for key in sql_dict:
            if not self.check_table_exist(key):
                print(f"table '{key}' does not exist, creating...", end="")
                sql = sql_dict[key]
                self.cur.execute(sql)
                self.con.commit()
                print("done.")

    def post_input(self, data: DataFrame):

        print('start post input...', end='')

        # post input
        post_data = data.drop(columns='topics')
        # 超過100字的內容歸零
        post_data['content'] = post_data['content'].apply(lambda s: "" if len(s) > 100 else s)
        post_data['title'] = post_data['title'].apply(lambda s: detag(s)[0])

        post_data.to_sql('post', self.con, if_exists='append', index=False)

        # post tag input
        columns = ['id', 'topics']
        tag_data = data.copy()
        tag_data['topics'] = \
            tag_data['topics'] + tag_data['title'].apply(lambda s: detag(s)[1])
        tag_data = tag_data[columns]
        tag_data = tag_data[tag_data['topics'].str.len() != 0]

        tag_temp = []
        for _, row in tag_data.iterrows():
            # TODO 討論或實驗一下要不要把中科大留下來
            tag_temp += [[row.id, topic] for topic in row.topics if topic != '中科大']

        a = DataFrame(
            data=tag_temp,
            columns=['post_id', 'tag_content']
        )

        a.to_sql('post_tag', con=self.con, if_exists='append', index=False)

        print('done')

    def comment_input(self, data: DataFrame):

        print('start comment input...', end='')



        print('done')


def data_parse(input_filename: str):
    with open(f'raw_data/{input_filename}', encoding='utf-8-sig') as csv_file:
        rows = DictReader(csv_file)
        rows = tuple(rows)[:100]

        for row in rows:
            post_id = row['id']
            title, tags = detag(row['title'])
            content, images = extract_image(row['content'])
            like_count = row['likeCount']
            topic = row['topic']

# raw_comment_input('dcard_nutc_comment_20190925_02.csv')
