# -*- coding: utf-8 -*-
"""
Created on 2024-01-13 11:27:56
---------
@summary:
---------
@author: dav
"""

import feapder
from curl_cffi import requests
import time
from lxml import etree
import json
from feapder import Item, UpdateItem
from parseData import parseData
import random
import polars as pl
from cookie_pool import GenCookie
from read_file import ReadFile


class OzonSpider(feapder.Spider):
    # 自定义数据库，若项目中有setting.py文件，此自定义可删除
    # __custom_setting__ = dict(
    #     REDISDB_IP_PORTS="192.168.0.106:6379", REDISDB_USER_PASS="", REDISDB_DB=0
    # )
    session = requests.Session()
    headers = {
        'authority': 'www.ozon.ru',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'zh-CN,zh;q=0.9',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    }
    accessToken = '4.160357014.QUOx4wbYSgmYs1I6de3aHQ.38.AVvZ4DMpCZD8Jo1Nq7IU05aKYRrx2ZhVcW9TkqMosX19WrAjVDbrGBFl9xl--axZJopJvYDPklmeh0MYKgVkQ9E.20240402125018.20240412062403.xdI4Pr4r5jkeXNZftxJI3BDityGhw6IfFbrxsj0atsE'

    # 这种方式获取的cookies 需要科学
    file_path = './data.csv'
    if file_path.endswith('.xlsx'):
        df = pl.read_excel(file_path,
                           schema_overrides={
                               "可用性 (%)": pl.Float64, '因缺货而错过的订单金额（₽）': pl.Float64, },
                           )
    elif file_path.endswith('.csv'):
        df = pl.read_csv(file_path,
                         dtypes={
                             "可用性 (%)": pl.Float64,
                             "因缺货而错过的订单金额（₽）": pl.Utf8,
                         }
                         )
        df = df.with_columns(
            pl.col("因缺货而错过的订单金额（₽）").map_elements(lambda x: x.replace(',', ''), return_dtype=pl.Float64)
        )

    def start_requests(self):
        gencookies = GenCookie()  # 实例化可以更换各个地方的cookie
        self.cookies, self.ua, self.headers = gencookies.gen_cookie()
        self.headers['x-o3-app-name'] = 'seller-ui'
        self.headers['x-o3-company-id'] = '1501369'
        self.headers['x-o3-language'] = 'zh-Hans'
        self.cookies['__Secure-access-token'] = self.accessToken

        url = 'https://seller.ozon.ru/api/site/seller-analytics/what_to_sell/data/v3'
        for row in self.df.iter_rows():
            id = row[self.df.columns.index("ID")]   # 这里的url是商品的ID
            print("获取商品ID：", id)
            json_data = {
                'filter': {
                    'stock': 'any_stock',
                    'sku': str(id),
                },
                'sort': {
                    'key': 'sum_rating',
                },
                'limit': '50',
                'offset': '0',
            }

            yield feapder.Request(url,
                                  method="POST",
                                  # headers=self.headers,
                                  # cookies=self.cookies,
                                  download_midware=self.download_midware2,
                                  callback=self.parse_list,
                                  meta={
                                      'json_data': json_data,
                                      'row': row
                                  }
                                  )

    def download_midware2(self, request):
        json_data = request.meta['json_data']

        response = self.session.post('https://seller.ozon.ru/api/site/seller-analytics/what_to_sell/data/v3',
                                     headers=self.headers,
                                     json=json_data,
                                     cookies=self.cookies,
                                     timeout=60)

        return request, response

    def parse_list(self, request, response):
        row = request.meta['row']
        # print(response.json())
        try:
            data = response.json()['items'][0]
            # print(data)
            item = Item()
            item.table_name = 'ozon_product'
            for i, col in enumerate(self.df.columns):
                item[col] = row[i]
            item['Imageurl'] = data['photo']
            print("商品图片：", data['photo'])
            yield item

        except Exception as e:
            print(e)


if __name__ == "__main__":
    OzonSpider(redis_key="ozon:OzonSpider").start()
