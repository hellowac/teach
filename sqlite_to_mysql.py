# encoding=utf-8

"""
    @Date       : 
    @Author     : wangchao
    Description : 将sqlite中的数据转移到mysql中
"""

from __future__ import unicode_literals, absolute_import

import sqlite3
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Unicode, UnicodeText, DateTime, Float
from sqlalchemy.dialects.mysql import LONGTEXT
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

# 查询有title没有detail的id
# select CONCAT('\'http://www.ireadweek.com/index.php/bookInfo/', id, '.html\',') as html from ireadweek_book_title where id not in (select title.id from ireadweek_book_detail as detail, `ireadweek_book_title` as title where detail.id = title.id);


class IreadWeekBookTitle(Base):
    __tablename__ = 'ireadweek_book_title'
    id = Column(Integer, primary_key=True)
    url = Column(String(255), default='')
    name = Column(String(255), default='')
    author = Column(String(255), default='')
    download_num = Column(Integer, default=0)


class IreadWeekBookDetail(Base):
    __tablename__ = 'ireadweek_book_detail'
    id = Column(Integer, primary_key=True)
    url = Column(String(255), default='')
    judge = Column(Float, default=0.0)          # 评分
    name = Column(String(255), default='')      # 书名
    author = Column(String(255), default='')
    tags = Column(String(255), default='')
    descr = Column(UnicodeText, default='')
    dwld_url = Column(String(255), default='')
    img_type = Column(String(10), default='jpg')
    img_name = Column(String(255), default='')
    img_bs4 = Column(LONGTEXT, default='')


conn_str = '{}+{}://{}:{}@{}:{}/{}'.format(
        'mysql', 'mysqldb', 'root', 'rootforwangchao', 'localhost', '3306', 'teach')

engine = create_engine(conn_str, echo=False)

DBSession = sessionmaker(bind=engine)

session = DBSession()

if not engine.dialect.has_table(engine, IreadWeekBookDetail.__tablename__):
    IreadWeekBookDetail.metadata.create_all(engine)  # 创建表.


def query_sqlite():
    conn = sqlite3.connect('dbs/ireadweek.sqlite', timeout=30000)

    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):

            key = col[0]
            if key == 'img':  # 旧数据teach.sqlite数据库 book_detail表的img字段在mysql中为img_bs4 (base64格式)
                d['img_bs4'] = row[idx]
            else:
                d[key] = row[idx]

        return d

    conn.row_factory = dict_factory
    cur = conn.cursor()

    from sqlalchemy import exists

    # update title
    cur.execute("select * from ireadweek_book_title")
    count = 0
    for record in cur.fetchall():

        existsed = session.query(exists().where(IreadWeekBookTitle.id == record['id'])).scalar()

        if not existsed:
            session.add(IreadWeekBookTitle(**record))
            count += 1

    session.commit()
    print(f'update title: {count}')

    # update detail
    count = 0
    cur.execute("select * from ireadweek_book_detail")
    for record in cur.fetchall():

        existsed = session.query(exists().where(IreadWeekBookDetail.id == record['id'])).scalar()

        if not existsed:
            session.add(IreadWeekBookDetail(**record))
            count += 1

    session.commit()
    print(f'update detail: {count}')


if __name__ == '__main__':
    query_sqlite()

