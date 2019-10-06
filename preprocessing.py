import re
from uuid import UUID

from sqlite3 import connect, register_converter, register_adapter
from typing import Tuple, Iterable, Generator, List
from pandas import DataFrame, read_sql_table
from sqlalchemy import create_engine, event


# TODO 改天用sqlalchemy重寫


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


def defloor(content: str) -> Tuple[str, List[int]]:
    pattern = r'B\d+'
    defloor_compile = re.compile(pattern)
    floor = list(map(lambda s: int(s[1:]), defloor_compile.findall(content)))
    result = defloor_compile.sub("", content).strip()

    return result, floor


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
        register_converter('GUID', lambda b: UUID(bytes_le=b))
        register_adapter(UUID, lambda u: u.bytes_le)

        self.con = connect(self.database_name)
        self.cur = self.con.cursor()

        self.engine = create_engine(f'sqlite:///{self.database_name}')

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
                    id GUID
                        constraint comment_pk
                            primary key,
                    post_id int not null
                        references post
                            on update cascade on delete cascade,
                    updated_at datetime not null ,
                    floor int not null,
                    content text not null,
                    like_count int not null
                );
            """,
            "response_comment": """
                create table response_comment
                (
                    source_comment_id GUID not null
                        references comment
                            on update cascade on delete cascade,
                    response_comment_id GUID
                        references comment
                            on update cascade on delete cascade,
                    constraint response_comment_pk
                        primary key (source_comment_id, response_comment_id)
                );
            """,
            "top_response": """
                create table top_response
                (
                    post_id int
                        references post
                            on update cascade on delete cascade,
                    comment_id GUID
                        references comment
                            on update cascade on delete cascade,
                    constraint top_response_pk
                        primary key (post_id, comment_id)
                );
            """
        }

        for key in sql_dict:

            if not self.check_table_exist(key):
                print(f"table '{key}' does not exist, creating...", end="")

                sql = sql_dict[key]
                self.cur.execute(sql)
                self.con.commit()

                print("done")

    def get_post(self) -> DataFrame:

        df = read_sql_table('post', self.engine)

        return df

    def post_input(self, data: DataFrame) -> None:

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

        tag_data = DataFrame(
            data=tag_temp,
            columns=['post_id', 'tag_content']
        )

        tag_data.to_sql('post_tag', con=self.con, if_exists='append', index=False)

        print('done')

    def comment_input(self, data: DataFrame) -> None:

        print(f'start {data["post_id"][0]} comment input...', end='')

        # comment_input
        comment_data = data.copy()
        comment_data['content'] = data['content'].apply(lambda s: defloor(s)[0])
        comment_data.to_sql('comment', con=self.con, if_exists='append', index=False)

        # response_comment_input
        columns = ['source_comment_id', 'response_comment_id']
        floor_to_id = {row.floor: row.id for _, row in data.iterrows()}
        floor_to_id.update({0: 0})

        floor = data['content'].apply(lambda s: defloor(s)[1])
        floor = floor.apply(lambda li: floor_to_id[li[0]] if len(li) == 1 else 0)
        floor = floor.rename('source_comment_id')

        res_comment_data = comment_data.join(floor)
        res_comment_data = res_comment_data[res_comment_data['source_comment_id'] != 0]
        res_comment_data = res_comment_data.rename(columns={'id': 'response_comment_id'})
        res_comment_data = res_comment_data[columns]
        res_comment_data.to_sql('response_comment', con=self.con, if_exists='append', index=False)

        # top_comment_input
        columns = ['post_id', 'id']
        top_comment_data = comment_data.join(floor)
        top_comment_data = top_comment_data[top_comment_data['source_comment_id'] == 0]

        if not top_comment_data.empty:
            top_comment_data = top_comment_data.sort_values('like_count', ascending=False)
            top_comment_data = top_comment_data[columns]

            top_response_length = len(top_comment_data) // 10
            top_comment_data = top_comment_data.loc[:top_response_length]
            top_comment_data = top_comment_data.rename(columns={'id': 'comment_id'})
            top_comment_data.to_sql('top_response', con=self.con, if_exists='append', index=False)

        print('done')
