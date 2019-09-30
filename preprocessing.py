import re
from csv import DictReader
from sqlite3 import connect
from typing import Tuple, Iterable, Generator, List


def extract_image(content: str) -> Tuple[str, list]:
    pattern = r"https:\/\/i\.imgur\.com\/.*.jpg"
    imgur_compile = re.compile(pattern)
    images = imgur_compile.findall(content)
    result = imgur_compile.sub("", content)

    return result, images


def detag(tittle: str) -> Tuple[str, Tuple[str, ...]]:
    pattern = r'#\w*'
    detag_compile = re.compile(pattern)
    tags: Tuple[str, ...] = tuple(map(lambda s: s[1:], detag_compile.findall(tittle)))
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


"""
post:{
    id: int
    title: str
    content: str
}

img_url:{
    post_id: int
    url: str
}

post_tag:{
    post_id: id
    tag_content: str 
}

comment:{
    comment_id: int
    post_id: int
    floor: int
    content: str    
}

response_comment:{
    comment_id: int
    response_comment_id: int
}

top_response:{
    post_id: int
    comment_id: int
}
"""


class Preprocess:

    def __init__(self, database_name: str):
        self.database_name = database_name

    def __enter__(self):
        self.con = connect(self.database_name)
        self.cur = self.con.cursor()
        return self

    def check_table_exist(self, table_name: str) -> bool:
        sql = f"""
            select name
            from sqlite_master
            where type='table' and name = '{table_name}'
        """

        self.cur.execute(sql)
        return bool(self.cur.fetchall())

    def check_database(self):
        sql_dict = {
            "post": """
                create table post
                (
                    id int not null
                        constraint post_pk
                            primary key,
                    title text not null,
                    content text not null
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
                create table comment_dg_tmp
                (
                    id int
                        constraint comment_pk
                            primary key autoincrement,
                    post_id int not null
                        references post
                            on update cascade on delete cascade,
                    floor int not null,
                    content text not null
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
                sql = sql_dict[key]
                self.cur.execute(sql)
                self.con.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()


def raw_comment_input(filename: str) -> None:
    con = connect('ddcard_nutc.sqlite3')
    cur = con.cursor()

    table_name = f"pre_raw_comment"
    sql = f"""
        select name
        from sqlite_master
        where type='table' and name = '{table_name}'
    """

    cur.execute(sql)

    if not cur.fetchall():
        sql = f"""
            create table {table_name}
            (
                id int
                    constraint pre_raw_comment_pk
                        primary key,
                post_id int not null,
                floor int not null,
                content text not null,
                like_count int
            );
        """

        cur.execute(sql)
        con.commit()

    with open(f'raw_data/{filename}', encoding='utf-8-sig') as csv_file:
        rows = DictReader(csv_file)
        rows = dict_filter_to_tuple(rows, 'createdAt', 'updatedAt')
        # rows = [tuple(row.values()) for row in rows]

        sql = """
            insert into pre_raw_comment
                (post_id, floor, content, like_count)
                values (?, ?, ?, ?);
        """

        cur.executemany(sql, rows)
        con.commit()

    con.close()


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


with Preprocess("dcard.splite3") as p:
    p.check_database()
