# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import base64
import sqlite3
from scrapy.exceptions import DropItem

from .spiders.iread import IreadSpider


class SqlitePipeline(object):

    def __init__(self, path, img_dir):
        self.sqlite_path = path
        self.img_dir = img_dir
        self.conn = None

    @classmethod
    def from_crawler(cls, crawler):
        # to create a pipeline instance from a Crawler

        return cls(
                crawler.settings.get('SQLITE_PATH'),        # sqlite文件地址
                crawler.settings.get('IMAGES_STORE'),       # images存储地址
        )

    def open_spider(self, spider):
        # called when the spider is opened

        self.conn = sqlite3.connect(self.sqlite_path, timeout=30000)

        # sqlite_master 表可以查询源信息
        check_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='book_title';"

        # 书籍title表: id, url, name, author, download_num
        book_title_table = """
            CREATE TABLE IF NOT EXISTS book_title (
              id INTEGER, 
              url TEXT, 
              name TEXT, 
              author TEXT, 
              download_num INTEGER);
        """

        # 书籍详细表: id, url, name, author, tags, judge, descr, img_type, img_name, img
        book_detail_table = """
            CREATE TABLE IF NOT EXISTS book_detail (
              id INTEGER, 
              url TEXT, 
              judge REAL, 
              name TEXT, 
              author TEXT, 
              tags TEXT, 
              descr TEXT, 
              dwld_url TEXT,
              img_type TEXT, 
              img_name TEXT,
              img TEXT);
        """

        self.conn.execute(book_title_table)
        self.conn.execute(book_detail_table)
        self.conn.commit()

    def close_spider(self, spider):
        # called when the spider is closed

        self.conn.close()

    def is_crawled_url(self, url, table='book_title'):
        """检查该url是否爬过，爬过则不爬"""

        query_sql = f"select url from {table} where url = '{url}';"

        cursor = self.conn.execute(query_sql)
        url_exists = bool(cursor.fetchone())

        return url_exists

    def is_crawled_detail(self, url):

        return self.is_crawled_url(url, table='book_detail')

    is_crawled_title = is_crawled_url

    def process_item(self, item, spider):

        if spider.name == IreadSpider.name:
            self.save_iread(item, spider)

        return item

    def save_iread(self, item, spider):
        """ 保存书籍的信息到sqlite """

        insert_sql = ''

        # 保存书籍title
        if item['table'] == 'title':
            if self.is_crawled_title(item['url']):
                raise DropItem(f'crawled title url: {item["url"]}')

            insert_sql = ("INSERT INTO book_title(id, url, name, author, download_num) "
                          "VALUES ({id}, '{url}', '{name}', '{author}', {download_num})").format(**item)

        # 保存书籍detail
        elif item['table'] == 'detail':
            if self.is_crawled_detail(item['url']):
                raise DropItem(f'crawled detail url: {item["url"]}')

            img_file = item['images'][0] if item['images'] else {}
            img_file = os.path.join(self.img_dir, img_file['path']) if img_file else ''
            img_64 = self.img_base64(img_file) if img_file else ''
            img_type = img_file.rpartition('.')[-1] if img_file else ''
            img_name = os.path.basename(img_file)

            # 删除该图片
            # os.remove(img_file)

            insert_sql = ("INSERT INTO book_detail(id, url, name, author, tags, judge, descr, dwld_url, img_type, img_name, img) "
                          "VALUES ({id}, '{url}', '{name}', '{author}', '{tags}', {judge}, '{descr}', '{dwld_url}', "
                          "'{img_type}', '{img_name}', '{img}')").format(
                          img_type=img_type, img_name=img_name, img=img_64, **item)

        self.conn.execute(insert_sql)
        self.conn.commit()

    def img_base64(self, img_file):
        """计算图片的base64表示

            html表示前缀: data:image/jpg;base64,{base64}
        """

        with open(img_file, 'br') as fr:
            b64 = base64.encodebytes(fr.read()).decode().replace('\n', '')

        return b64



