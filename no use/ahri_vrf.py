import requests
from requests_html import HTMLSession
from requests.exceptions import RequestException
import json
import time
import pymongo
from pymongo.errors import DuplicateKeyError
from commonlog import CommonLog
from zhimaProxy import *
import random
from fake_useragent import UserAgent

client = pymongo.MongoClient(host='localhost', port=27017)
db = client['eurovent']
product_collection = db['ahri_products']

'''
1. 发送get请求搜索页获取cookie，再发送post请求api
2. 获取首页response和搜索结果数，从而得到页数
2. 发送后面页数的request，若遇到状态码500，换ua，获取新cookie，继续爬取剩余页面
3. 将response储存到MongoDB
'''

# Commercial - Variable Refrigerant Flow (VRF) Air-Conditioners and Heat Pumps
class Ahri:

    def __init__(self):
        self.log = CommonLog().log()
        self.sum = 0
        self.session = HTMLSession()

    def get_session(self):
        # 发起get请求，获取新cookie
        a = self.session.get(
            url='https://www.ahridirectory.org/NewSearch?programId=72&searchTypeId=3',
            headers={'User-Agent': UserAgent().random})
        time.sleep(random.randint(5, 10))

    def get_headers(self):
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Host': 'www.ahridirectory.org',
            'Origin': 'https://www.ahridirectory.org',
            'Referer': 'https://www.ahridirectory.org/NewSearch?programId=72&searchTypeId=3',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': UserAgent().random,
            'X-Requested-With': 'XMLHttpRequest'
        }
        return headers

    def get_page(self, draw, start):
        """
        抓取索引页
        :param start:
        :param draw:
        """
        '''
        发送请求后，
        若返回的响应状态码为200，则使用同一session发送下一页请求；
        若返回的响应状态码不为200，则需要更换session，重新发送该页请求
        '''

        payload = {
                      "draw": draw,
                      "columns": [
                        {
                          "data": 0,
                          "name": "ReferenceId",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 1,
                          "name": "ModelStatusId",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 2,
                          "name": "BrandId",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 3,
                          "name": "SeriesName",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 4,
                          "name": "ARITypeidVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 5,
                          "name": "SystemModelNumberVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 6,
                          "name": "ModuleModelNumber1VRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 7,
                          "name": "ModuleModelNumber2VRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 8,
                          "name": "ModuleModelNumber3VRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 9,
                          "name": "ModuleModelNumber4VRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 10,
                          "name": "ModuleModelNumber5VRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 11,
                          "name": "ModuleModelNumber6VRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 12,
                          "name": "ModuleModelNumber7VRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 13,
                          "name": "IndoorTypeVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 14,
                          "name": "Capacity95FVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 15,
                          "name": "EER95FVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 16,
                          "name": "Capacity86FVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 17,
                          "name": "EER86FVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 18,
                          "name": "IEERVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 19,
                          "name": "SCHEVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 20,
                          "name": "HighHeat47FVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 21,
                          "name": "HighCOP47FVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 22,
                          "name": "LowHeatVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 23,
                          "name": "LowCOP17FVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 24,
                          "name": "HeatCap68FVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 25,
                          "name": "COP68FVRF",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 26,
                          "name": "SoldIn",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        },
                        {
                          "data": 27,
                          "name": "IsRerated",
                          "searchable": True,
                          "orderable": True,
                          "search": {
                            "value": "",
                            "regex": False
                          }
                        }
                      ],
                      "order": [
                        {
                          "column": 0,
                          "dir": "asc"
                        }
                      ],
                      "start": start,
                      "length": 250,
                      "search": {
                        "value": "",
                        "regex": False
                      },
                      "formData": "hdnProgramId=72&hdnSearchTypeId=3&productType_id=&__RequestVerificationToken=qTXtqXVqO-CZOkaXuwriS276yVx7uoQdjjp5R3uCK0Gn5fhgFsiZxYMIotBmjx5uLGU2kt1k5KqUEi54ktA6VYH20Jw1&ReferenceId=&SeriesName=&ARITypeidVRF=&SystemModelNumberVRF=&ModuleModelNumber1VRF=&ModuleModelNumber2VRF=&ModuleModelNumber3VRF=&ModuleModelNumber4VRF=&ModuleModelNumber5VRF=&ModuleModelNumber6VRF=&ModuleModelNumber7VRF=&IndoorTypeVRF=&IsRerated="
                    }

        data = json.dumps(payload)

        url = 'https://www.ahridirectory.org/NewSearch/Search'

        try:
            self.log.info(f'发送第{draw}页请求')
            response = self.session.post(url=url, data=data, timeout=300)
            #  若返回的响应状态码为200，则使用同一session发送下一页请求；
            if response.status_code == 200:
                self.log.info(f'获取第{draw}页响应成功')
                page = response.json()
                return page
            # 若返回的响应状态码不为200，则需要更换session，重新发送该页请求
            else:
                status_code = response.status_code
                self.log.error(f'获取第{draw}页响应失败，状态码{status_code}，更换session，继续发送第{draw}页请求')
                # proxies = zhimaProxy().getIP()
                # session.proxies = proxies
                # 更换cookie和ua
                self.get_session()
                self.session.headers.update(self.get_headers())
                time.sleep(random.randint(5, 10))
                self.get_page(draw, start)
        # 若请求异常，更换cookie和ua，重新发送该页请求
        except RequestException as err:
            self.log.error(f'第{draw}页请求异常：{err}，继续发送第{draw}页请求')
            # 更换cookie和ua
            self.get_session()
            self.session.headers.update(self.get_headers())
            time.sleep(random.randint(5, 10))
            self.get_page(draw, start)

    def save_to_mongo(self, item):
        crawl_date = time.strftime("%Y-%m-%d", time.localtime())  # 运行当天的日期
        try:
            # AHRI Certified Reference Number(RN)存在则不插入，但其他字段不完全相同时会更新其他字段，RN不存在则插入新文档
            result = product_collection.update_one({"AHRI Certified Reference Number":
                                                        item['AHRI Certified Reference Number']},
                                                   {"$set": item}, upsert=True)
        except DuplicateKeyError:
            pass
        else:
            upserted_id = result.upserted_id  # 用于判断是插入或更新操作
            if upserted_id is not None:
                # 如果插入新文档，需要向文档更新crawl_date
                product_collection.update_one(item, {"$set": {"crawl_date": crawl_date}})
                self.log.info(f"RN:{item['AHRI Certified Reference Number']}不存在，已插入")
            elif upserted_id is None:
                if result.matched_count == 1:
                    if result.modified_count == 0:
                        self.log.info(f"RN:{item['AHRI Certified Reference Number']}已存在，无更新")
                    elif result.modified_count == 1:
                        self.log.info(f"RN:{item['AHRI Certified Reference Number']}已存在，已更新")

    def sum_saved_records(self, count):
        self.sum = self.sum + count

    def get_champ(self, page):
        rows = page["dataRaw"]
        count = 0
        for row in rows:
            item = {}
            item['Product Type'] = 'Variable Refrigerant Flow (VRF) Air-Conditioners and Heat Pumps'
            item['AHRI Certified Reference Number'] = row[0]
            item['Model Status'] = row[1]
            item['Brand Name'] = row[2]
            item['Series Name'] = row[3]
            item['AHRI Type'] = row[4]
            item['System Model Number'] = row[5]
            item['Module Model Number 1'] = row[6]
            item['Module Model Number 2'] = row[7]
            item['Module Model Number 3'] = row[8]
            item['Module Model Number 4'] = row[9]
            item['Module Model Number 5'] = row[10]
            item['Module Model Number 6'] = row[11]
            item['Module Model Number 7'] = row[12]
            item['Indoor Type'] = row[13]
            item['AHRI Certified Ratings - Cooling Capacity (95F)'] = row[14]
            item['AHRI Certified Ratings - EER (95F)'] = row[15]
            item['AHRI Certified Ratings - Cooling Capacity (86F)'] = row[16]
            item['AHRI Certified Ratings - EER (86F)'] = row[17]
            item['IEER'] = row[18]
            item['SCHE'] = row[19]
            item['AHRI Certified Ratings - High Heating Capacity (47F)'] = row[20]
            item['AHRI Certified Ratings - High COP (47F)'] = row[21]
            item['AHRI Certified Ratings - Low Heating Capacity (17F)'] = row[22]
            item['AHRI Certified Ratings - Low COP (17F)'] = row[23]
            item['AHRI Certified Ratings - Heating Capacity (68F)'] = row[24]
            item['AHRI Certified Ratings - COP (68F)'] = row[25]
            item['Sold In?'] = row[26]
            item['Is Rerated'] = row[27]

            count = count + 1
            # 将每条产品记录储存到MongoDB中
            self.save_to_mongo(item)

        self.sum_saved_records(count)
        self.log.info(f'本页有{count}条结果')

    def main(self):
        # 生成第一次get请求，获取cookie，更新headers
        self.get_session()
        self.session.headers.update(self.get_headers())
        first_page = self.get_page(1, 0)  # 返回字典格式
        records_filtered = first_page["recordsFiltered"]  # 获取条件过滤的产品数，有44页结果，10808条结果
        self.log.info(f'有{records_filtered}条结果')
        self.get_champ(first_page)  # 爬取第一页
        # 获取产品页面数
        if records_filtered % 250 == 0:
            page_num = records_filtered // 250
        else:
            page_num = (records_filtered // 250) + 1
        self.log.info(f'有{page_num}页,完成第1页')

        # 循环抓取产品每一页
        for i in range(2, page_num+1):
            start = (i-1) * 250
            page = self.get_page(i, start)
            self.get_champ(page)
            self.log.info(f'有{page_num}页,完成第{i}页')
            time.sleep(random.randint(10, 15))

        self.log.info(f'爬取完成,已保存{self.sum}条结果')


if __name__ == '__main__':
    p = Ahri()
    p.main()