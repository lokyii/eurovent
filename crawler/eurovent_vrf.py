import random
import requests
from fake_useragent import UserAgent
from requests.exceptions import RequestException
import json
import time
import pymongo
from pymongo.errors import DuplicateKeyError
from pyquery import PyQuery as pq
from requests_html import HTMLSession
from commonlog import CommonLog
from zhimaProxy import zhimaProxy

# 仅爬取vrf和eurovent-vrf认证产品

client = pymongo.MongoClient(host='localhost', port=27017)
# 数据库登录
admin_db = client['admin']
user = 'admin'
pwd = 'admin888&&'
admin_db.authenticate(user, pwd)

db = client['eurovent']
product_collection = db['eurovent_vrf_insertall_20220301']
champ_collection = db['champ']
# 建立索引，建立后请注释掉本行；product_id会变化，故不选择此作为index
# product_collection.create_index([('product_family_abbr', 1), ('product_type_abbr', 1), ('brand', 1), ('range', 1),
#                                  ('model_name', 1)], unique=True, background=True)

crawl_date = time.strftime("%Y-%m-%d", time.localtime())  # 运行当天的日期


class Eurovent:

    def __init__(self, product_family, product_type, product_family_text, product_type_text, dict):
        self.product_family = product_family
        self.product_type = product_type
        self.product_family_text = product_family_text
        self.product_type_text = product_type_text
        self.dict = dict
        self.log = CommonLog().log()
        self.session = HTMLSession()

        self.log.info("插入EUROVENT VRF全部产品")

    def get_session(self):
        # 发起get请求，获取新cookie
        r = self.session.get(
            url='https://www.eurovent-certification.com/en/',
            headers={'User-Agent': UserAgent().random})
        time.sleep(random.randint(5, 10))

    def get_page(self, offset):
        """
        抓取索引页
        :param offset:
        :return: result:
        """
        headers = {
            'User-Agent': UserAgent().random
        }
        result_url = 'https://www.eurovent-certification.com/en/advancedsearch/ajax'
        data = {
            'limit': "15",
            'offset': offset,
            'program': self.product_family,
            'product_type': self.product_type,
            'keyword': ""
        }

        try:
            response = self.session.post(url=result_url, data=data, headers=headers)
            if response.status_code == 200:
                page = response.json()
                return page
            else:
                self.get_page(offset)
        except RequestException as err:
            self.log.error(f'{self.product_family},{self.product_type}第{(offset//15)+1}页请求异常：{err}')
            self.get_page(offset)

    def save_to_mongo(self, item):
        # 首次插入数据代码开始
        try:
            product_collection.insert_one(item)
        except DuplicateKeyError:
            self.log.info(
                f'{self.product_family},{self.product_type},{item["brand"]},{item["range"]},{item["model_name"]},'
                f'{item["IU Range Names [General - Product]"], item["IU Names [General - Product]"]},爬取日期{item["crawl_date"]}重复')
        else:
            self.log.info(
                f'{self.product_family},{self.product_type},{item["brand"]},{item["range"]},{item["model_name"]},'
                f'{item["IU Range Names [General - Product]"], item["IU Names [General - Product]"]},爬取日期{item["crawl_date"]},插入到MongoDB成功')
        # 首次插入数据代码结束


    def get_champ(self, page):
        """
        提取产品技术参数
        :param page:
        :return:
        """
        for item in page['rows']:
            # 将字典中的键'id'改为'product_id'
            item['product_id'] = item.pop('id')
            # 删除元素ppr
            del item['ppr']

            # 列表记录value='-'对应的key
            keys_to_delete = []

            item['product_family'] = self.product_family_text
            item['product_type'] = self.product_type_text
            item['product_family_abbr'] = self.product_family
            item['product_type_abbr'] = self.product_type
            l = pq(item.get('model_name')).text().replace(u'\xa0', u' ').replace(' ', '').split()
            item['model_name'] = l[0]
            item['deleted_or_new'] = l[1] if len(l) == 2 else None
            item['crawl_date'] = crawl_date

            # VRF有些产品iu_names和iu_range_names为“-”，但为了区分产品不能删除这两个参数
            # 找出iu_names和iu_range_names的champ_id
            if self.product_family == "VRF":
                for key, value in self.dict.items():
                    if value == "IU Names [General - Product]":
                        iu_names_champ_id = key
                    if value == "IU Range Names [General - Product]":
                        iu_range_names_champ_id = key

            for key, value in item.items():
                # 当key包含'champ_'字符串，则该key为技术参数，取value<span>中的值
                if 'champ_' in key:
                    item[key] = pq(item.get(key)).text()
                # 当value='-'，将key添加到keys_to_delete
                if value == '-':
                    if self.product_family == "Eurovent-HP":
                        keys_to_delete.append(key)
                    elif self.product_family == "VRF" and key != iu_names_champ_id and key != iu_range_names_champ_id:
                        keys_to_delete.append(key)

            # 删除value='-'的元素
            for i in keys_to_delete:
                del item[i]

            # 遍历时不能修改字典元素的解决办法：将遍历条件改为列表
            for key in list(item.keys()):
                if 'champ_' in key:
                    # 当key包含'champ_'字符串，则替换为该技术参数的名称
                    item[self.dict[key]] = item.pop(key)
            # 将每条产品记录储存到MongoDB中
            self.save_to_mongo(item)

    def main(self):
        # 生成第一次get请求，获取cookie
        self.get_session()
        first_page = self.get_page("0")  # 返回字典格式
        records_filtered = first_page["total"]  # 获取条件过滤的产品数
        self.log.info(f'{self.product_family},{self.product_family_text},{self.product_type},{self.product_type_text}有{records_filtered}条结果')
        self.get_champ(first_page)  # 爬取第一页

        # 获取产品页面数
        if records_filtered > 15:
            if records_filtered % 15 == 0:
                page_num = records_filtered // 15
            else:
                page_num = (records_filtered // 15) + 1
            self.log.info(f'正在爬取{self.product_family},{self.product_family_text}）,{self.product_type},{self.product_type_text},共有{page_num}页,完成第1页')

            # 循环抓取产品每一页
            for i in range(1, page_num):
                offset = i * 15
                page = self.get_page(offset)
                self.get_champ(page)
                self.log.info(f'正在爬取{self.product_family},{self.product_family_text},{self.product_type},{self.product_type_text},共有{page_num}页,完成第{i + 1}页')
                time.sleep(random.randint(5, 10))
            self.log.info(f'{self.product_family}-{self.product_type}爬取完成')

        else:
            self.log.info(f'{self.product_family},{self.product_family_text},{self.product_type},{self.product_type_text}结果只有1页,爬取完成')


class Products:

    def __init__(self):
        # 把各product_family的技术参数ID和值放到字典中
        self.vrf_dict = {}
        self.eurovent_hp_dict = {}

        for champ in champ_collection.find({"crawl_date": "2022-02-25"}):
            if champ['product_family'] == 'VRF':
                self.vrf_dict[champ['champ_id']] = champ['new_champ_name']
            elif champ['product_family'] == 'Eurovent-HP':
                self.eurovent_hp_dict[champ['champ_id']] = champ['new_champ_name']

    def main(self):
        # 导入需要爬虫产品的json：VRF和Eurovent Heat Pump
        input_filename = '../products_vrf.json'
        with open(input_filename) as f:
            products = json.load(f)

        # 循环爬取每类产品，若之前因为报错停止运行，则从爬取完成产品的下一个产品开始爬取
        for product in products['products']:
            if product['product_family'] == 'VRF':
                obj = Eurovent(product['product_family'], product['product_type'], product['product_family_text'],
                               product['product_type_text'], self.vrf_dict)
            elif product['product_family'] == 'Eurovent-HP':
                obj = Eurovent(product['product_family'], product['product_type'], product['product_family_text'],
                               product['product_type_text'], self.eurovent_hp_dict)

            obj.main()


if __name__ == '__main__':
    p = Products()
    p.main()






