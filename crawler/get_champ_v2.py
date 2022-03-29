import random
import pymongo
from pymongo.errors import DuplicateKeyError
import json
import time
from commonlog import CommonLog
from zhimaProxy import *
from fake_useragent import UserAgent
from lxml import etree
import requests
"""
    产品技术参数时不时发生改变，需在产品技术参数发生改变时执行
"""


class Champ:
    def __init__(self):
        # proxy = None
        # while proxy is None:
        #     proxy = zhimaProxy().getIP()

        self.log = CommonLog().log()

        client = pymongo.MongoClient(host='localhost', port=27017)
        # 数据库登录
        admin_db = client['admin']
        user = 'admin'
        pwd = 'admin888&&'
        admin_db.authenticate(user, pwd)

        db = client['eurovent']
        self.collection = db['champ']
        # 建立索引，建立后请注释掉本行
        # self.collection.create_index([('product_family', 1), ('product_type', 1), ('champ_id', 1), ('crawl_date', 1)], unique=True, background=True)

    def index_page(self, product_family, product_type):
        """
        抓取索引页
        :param product_family:
        :param product_type:
        :return:
        """
        self.log.info(f'正在获取{product_family}-{product_type}的技术参数')

        try:
            url = f'https://www.eurovent-certification.com/en/advancedsearch/result?program={product_family}&product_type={product_type}&keyword=#access-results'
            response = requests.get(url, headers={'User-Agent': UserAgent().random})

        # except TimeoutException:
        except Exception:
            self.index_page(product_family, product_type)
        else:
            self.get_champ(response, product_family, product_type)

    def get_champ(self, response, product_family, product_type):
        """
        提取产品技术参数
        """
        selector = etree.HTML(response.text)
        items = selector.xpath('//tr[@class="sub-header"]/th')

        crawl_date = time.strftime("%Y-%m-%d", time.localtime())

        for item in items:
            champ_type = item.xpath('./span[1]/@id')[0]
            champ_name = " ".join(i.strip() for i in item.xpath('./span//text()'))
            champ = {
                'crawl_date': crawl_date,
                'product_family': product_family,
                'product_type': product_type,
                'champ_id': item.xpath('./@data-field')[0],
                'champ_type': champ_type,
                'champ_title': item.xpath('./span[1]/@title')[0],
                'champ_name': champ_name,  # 去掉字符串结尾空格
                'new_champ_name': champ_name + ' [' + champ_type + ']'
            }

            self.log.info(champ)
            self.save_to_mongo(champ)

    def save_to_mongo(self, champ):
        """
        保存至MongoDB
        :param champ:
        """

        try:
            # product_family + product_type + champ_id + crawl_date存在则不插入，但其他字段不完全相同时会更新其他字段，不存在则插入新文档
            result = self.collection.update_one({"product_family": champ['product_family'],
                                                 "product_type": champ['product_type'],
                                                 "champ_id": champ['champ_id'],
                                                 "crawl_date": champ['crawl_date']}, {"$set": champ}, upsert=True)
        except DuplicateKeyError:
            pass
        else:
            upserted_id = result.upserted_id  # 用于判断是插入或更新操作
            if upserted_id is not None:
                # 如果插入新文档，需要向文档更新crawl_date。但由于try语句已经插入crawl_date字段，无需执行下一行。
                # self.collection.update_one(champ, {"$set": {"crawl_date": crawl_date}})
                self.log.info(f"{champ['product_family']}, {champ['product_type']}, {champ['champ_id']}, {champ['crawl_date']}不存在，已插入")
            elif upserted_id is None:
                if result.matched_count == 1:
                    if result.modified_count == 0:
                        self.log.info(f"{champ['product_family']}, {champ['product_type']}, {champ['champ_id']}, {champ['crawl_date']}已存在，无更新")
                    elif result.modified_count == 1:
                        self.log.info(f"RN:{champ['product_family']}, {champ['product_type']}, {champ['champ_id']}, {champ['crawl_date']}已存在，已更新")

    def main(self):
        """
        主入口
        """
        # 1. 导入需要爬虫产品的json
        input_filename = '../products_vrf_n_hp.json'
        with open(input_filename) as f:
            products = json.load(f)

        # 2. 遍历每个产品执行爬虫
        for product in products['products']:
            self.index_page(product['product_family'], product['product_type'])
            time.sleep(random.randint(5, 10))


if __name__ == '__main__':
    champs = Champ()
    champs.main()