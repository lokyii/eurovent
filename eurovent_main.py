import requests
from requests.exceptions import RequestException
import json
import time
import pymongo
from pymongo.errors import DuplicateKeyError
from pyquery import PyQuery as pq

client = pymongo.MongoClient(host='localhost', port=27017)
db = client['eurovent']
product_collection = db['products']
champ_collection = db['champ']
# 建立索引，建立后请注释掉本行
# product_collection.create_index([('product_id', 1), ('crawl_date', 1)], unique=True, background=True)


class Eurovent:

    def __init__(self, product_family, product_type, product_family_text, product_type_text, dict):
        self.product_family = product_family
        self.product_type = product_type
        self.product_family_text = product_family_text
        self.product_type_text = product_type_text
        self.dict = dict

    def get_page(self, offset):
        """
        抓取索引页
        :param offset:
        :return: result:
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0'
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
            response = requests.post(url=result_url, data=data, headers=headers)
            if response.status_code == 200:
                page = response.json()
                return page
        except Exception as err:
            print(f'{self.product_family},{self.product_type}第{(offset//15)+1}页请求异常：{err}')
            self.get_page(offset)

    def save_to_mongo(self, item):
        try:
            product_collection.insert_one(item)
        except Exception as err:
            print(f'{self.product_family},{self.product_type},{item["product_id"]},{item["brand"]},{item["model_name"]},爬取日期{item["crawl_date"]},存储失败:{err}')
        else:
            print(f'{self.product_family},{self.product_type},{item["product_id"]},{item["brand"]},{item["model_name"]},爬取日期{item["crawl_date"]},存储到MongoDB成功')

    def get_champ(self, page):
        """
        提取产品技术参数
        :param page:
        :return:
        """
        # 当天运行的代码出错，但有部分产品的数据已经储存在MongoDB中，重新运行时跳过这些储存过产品
        crawl_date = time.strftime("%Y-%m-%d", time.localtime())  # 运行当天的日期
        saved_products = []
        for product in product_collection.find({'crawl_date': crawl_date}):
            saved_products.append(product['product_id'])
        # print('保存的产品', saved_products)

        for item in page['rows']:
            if item['id'] in saved_products:
                print(item['id'], '今天已经保存了')
                continue
            else:
                # 将字典中的键'id'改为'product_id'
                item['product_id'] = item.pop('id')
                # 删除元素ppr
                del item['ppr']

                # 列表记录value='-'对应的key
                keys_to_delete = []

                item['crawl_date'] = crawl_date
                item['product_family'] = self.product_family_text
                item['product_type'] = self.product_type_text
                item['product_family_abbr'] = self.product_family
                item['product_type_abbr'] = self.product_type
                l = pq(item.get('model_name')).text().replace(u'\xa0', u' ').replace(' ', '').split()
                item['model_name'] = l[0]
                item['deleted_or_new'] = l[1] if len(l) == 2 else None

                for key, value in item.items():
                    # 当key包含'champ_'字符串，则该key为技术参数，取value<span>中的值
                    if 'champ_' in key:
                        item[key] = pq(item.get(key)).text()
                    # 当value='-'，将key添加到keys_to_delete
                    if value == '-':
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
        first_page = self.get_page("0")  # 返回字典格式
        records_filtered = first_page["total"]  # 获取条件过滤的产品数
        print(f'{self.product_family},{self.product_family_text},{self.product_type},{self.product_type_text}有{records_filtered}条结果')
        self.get_champ(first_page)  # 爬取第一页

        # 获取产品页面数
        if records_filtered > 15:
            if records_filtered % 15 == 0:
                page_num = records_filtered // 15
            else:
                page_num = (records_filtered // 15) + 1
            print(f'正在爬取{self.product_family},{self.product_family_text}）,{self.product_type},{self.product_type_text},共有{page_num}页,完成第1页')

            # 循环抓取产品每一页
            for i in range(1, page_num):
                offset = i * 15
                page = self.get_page(offset)
                self.get_champ(page)
                print(f'正在爬取{self.product_family},{self.product_family_text},{self.product_type},{self.product_type_text},共有{page_num}页,完成第{i + 1}页')
            print(f'{self.product_family}-{self.product_type}爬取完成')

        else:
            print(f'{self.product_family},{self.product_family_text},{self.product_type},{self.product_type_text}结果只有1页,爬取完成')


class Products:

    def __init__(self):
        # 把各product_family的技术参数ID和值放到字典中
        self.vrf_dict = {}
        self.eurovent_hp_dict = {}
        self.msc007_dict = {}
        self.nf414_dict = {}
        for champ in champ_collection.find({'crawl_date': '2021-10-26'}):
            if champ['product_family'] == 'VRF':
                self.vrf_dict[champ['champ_id']] = champ['champ_name']
            elif champ['product_family'] == 'Eurovent-HP':
                self.eurovent_hp_dict[champ['champ_id']] = champ['champ_name']
            elif champ['product_family'] == 'MCS007':
                self.msc007_dict[champ['champ_id']] = champ['champ_name']
            elif champ['product_family'] == 'NF414':
                self.nf414_dict[champ['champ_id']] = champ['champ_name']

    def main(self):
        # 1. 导入需要爬虫产品的json
        input_filename = 'products.json'
        with open(input_filename) as f:
            products = json.load(f)

        # 2. 循环爬取每类产品
        for product in products['products']:
            if product['product_family'] == 'VRF':
                obj = Eurovent(product['product_family'], product['product_type'], product['product_family_text'],
                               product['product_type_text'], self.vrf_dict)
            elif product['product_family'] == 'Eurovent-HP':
                obj = Eurovent(product['product_family'], product['product_type'], product['product_family_text'],
                               product['product_type_text'], self.eurovent_hp_dict)
            elif product['product_family'] == 'MCS007':
                obj = Eurovent(product['product_family'], product['product_type'], product['product_family_text'],
                               product['product_type_text'], self.msc007_dict)
            elif product['product_family'] == 'NF414':
                obj = Eurovent(product['product_family'], product['product_type'], product['product_family_text'],
                               product['product_type_text'], self.nf414_dict)

            obj.main()


if __name__ == '__main__':
    p = Products()
    p.main()






