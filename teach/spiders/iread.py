# -*- coding: utf-8 -*-

import sqlite3
import scrapy
from pyquery import PyQuery


class IreadSpider(scrapy.Spider):
    name = 'iread'
    allowed_domains = ['www.ireadweek.com']
    start_urls = [
        'http://www.ireadweek.com/',
    ]

    def parse(self, response):
        pqhtml = PyQuery(response.text)

        books = pqhtml('ul.hanghang-list a').items()

        detail_urls = []

        for book in books:
            sub_url = response.urljoin(book.attr('href'))
            book_id = sub_url.rpartition('/')[-1].split('.')[0]

            detail_urls.append(sub_url)      # 该本书籍详细url

            yield {
                'table': 'title',
                'id': book_id,
                'url': sub_url,
                'name': book('div.hanghang-list-name').text(),
                'author': book('div.hanghang-list-zuozhe').text(),
                'download_num': book('div.hanghang-list-num').text(),
            }

        # 下一页列表
        pages = pqhtml('nav.action-pagination li a').items()
        for page in pages:
            if page.text() == '下一页>>':
                next_page = response.urljoin(page.attr('href'))
                yield scrapy.Request(next_page, callback=self.parse)
                break

        # 新的书籍详细页面
        for detail_url in detail_urls:
            yield scrapy.Request(detail_url, callback=self.parse_detail)

    def parse_detail(self, response):
        # 解析书籍详细页
        pqhtml = PyQuery(response.text)

        img_src = pqhtml('div.hanghang-shu-content-img img').attr('src')
        img_url = response.urljoin(img_src) if img_src.endswith('.jpg') else None

        name = pqhtml('div.hanghang-za-title:first').text()
        descr = pqhtml('div.hanghang-shu-content-font p:last').text()
        download_url = pqhtml('div.hanghang-shu-content-btn a.downloads').attr('href')
        _id = response.url.rpartition('/')[-1].split('.')[0]

        author = None
        tags = None
        judge = None

        ps = pqhtml('div.hanghang-shu-content-font p:lt(3)').items()
        for p in ps:
            txt = p.text()

            _l = txt.split('：') if '：' in txt else ['unknow', '']
            k, v = _l[:2]

            if k == '作者':
                author = v
            elif k == '分类':
                tags = ';'.join(v.split())
            elif k == '豆瓣评分':
                judge = v

        yield {
            'table': 'detail',
            'id': _id,
            'url': response.url,
            'name': name,
            'author': author,
            'tags': tags,
            'judge': float(judge) if judge else 0,
            'descr': descr.replace("'", "\'"),
            'dwld_url': download_url,
            'image_urls': [img_url, ] if img_url else [],
        }




