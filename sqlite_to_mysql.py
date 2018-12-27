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


class Task:
    __tablename__ = 'celery_flower_task'
    id = Column(Integer, primary_key=True)
    pid = Column(Integer, default=0)                    # 执行任务的进程ID.
    uuid = Column(String(255), unique=True)             # 任务ID
    name = Column(String(255), default='')              # 任务名称
    hostname = Column(String(255), default='')          # 执行任务的机器
    state = Column(String(255), default='')             # 任务状态
    args = Column(Unicode(255), default='')             # 任务位置参数
    kwargs = Column(Unicode(255), default='')           # 任务关键字参数
    result = Column(UnicodeText(), default='')          # 任务结果
    expired = Column(String(255), default='False')      # 任务是否过期
    signum = Column(String(255), default='')            # 取消任务时使用的信号
    retries = Column(Integer, default=0)                # 重试了几次
    rejected = Column(String(255), default='')          # 是否被取消
    exception = Column(UnicodeText, default='')         # 发生的异常描述
    traceback = Column(UnicodeText, default='')         # 调用栈(发生异常时)
    timestamp = Column(Float, nullable=True, default=0)            # 发生时间.
    runtime = Column(Float, nullable=True, default=0)   # 运行时间.
    started = Column(DateTime(), nullable=True, default=None)       # 开始时间
    received = Column(DateTime(), nullable=True, default=None)      # 到达时间
    succeeded = Column(DateTime(), nullable=True, default=None)     # 成功时间
    failed = Column(DateTime(), nullable=True, default=None)        # 失败时间
    eta = Column(String(255), default='')               # 预计执行时间
    retried = Column(String(255), default='')           # 是否重试过
    parent_id = Column(String(255), default='')         # 父任务ID
    root_id = Column(String(255), default='')           # 根任务ID


class IreadWeekBoookTitle(Base):
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

engine = create_engine(conn_str, echo=True)

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
    cur.execute("select * from book_title")
    # cur.execute("select * from book_detail")

    from sqlalchemy import exists

    for record in cur.fetchall():

        existsed = session.query(exists().where(IreadWeekBookDetail.id == record['id'])).scalar()

        if not existsed:
            session.add(IreadWeekBoookTitle(**record))
            # session.add(IreadWeekBookDetail(**record))

    session.commit()


if __name__ == '__main__':
    query_sqlite()