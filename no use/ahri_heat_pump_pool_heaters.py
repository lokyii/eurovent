import requests
from requests.exceptions import RequestException
import json
import time
import pymongo
from pymongo.errors import DuplicateKeyError
from pyquery import PyQuery as pq
from commonlog import CommonLog
from zhimaProxy import *

client = pymongo.MongoClient(host='localhost', port=27017)
db = client['eurovent']
product_collection = db['ahri_products']
# 建立索引，建立后请注释掉本行
# product_collection.create_index([('AHRI Certified Reference Number', 1)], unique=True,
#                                 background=True)

'''
准备动作：代理IP
1. 发送首页request，获取首页response和搜索结果数，从而得到页数
2. 发送后面页数的request
3. 将response储存到MongoDB
'''


class Ahri:

    def __init__(self):
        self.log = CommonLog().log()
        self.sum = 0

    def get_page(self, draw, start, session):
        """
        抓取索引页
        :param start:
        :param draw:
        :param session:
        :return: page:
        """
        '''
        发送请求后，
        若返回的响应状态码为200，则使用同一IP发送下一页请求；
        若返回的响应状态码不为200，则需要更换IP，重新发送该页请求
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
              "name": "OldRefId",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 2,
              "name": "ModelStatusId",
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
              "name": "BrandId",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 5,
              "name": "ModelNumberHPPH",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 6,
              "name": "ModelNumber2",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 7,
              "name": "AHRIType",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 8,
              "name": "RefrigerantType",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 9,
              "name": "HighAirTemperatureHighHumidityHeatCapacity",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 10,
              "name": "HighAirTemperatureHighHumidityCOP",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 11,
              "name": "HighAirTemperatureMidHumidityHeatCapacity",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 12,
              "name": "HighAirTemperatureMidHumidityCOP",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 13,
              "name": "LowAirTemperatureMidHumidityHeatCapacity",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 14,
              "name": "LowAirTemperatureMidHumidityCOP",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 15,
              "name": "Volts",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 16,
              "name": "Phase",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 17,
              "name": "Hertz",
              "searchable": True,
              "orderable": True,
              "search": {
                "value": "",
                "regex": False
              }
            },
            {
              "data": 18,
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
          "formData": "hdnProgramId=36&hdnSearchTypeId=3&productType_id=&__RequestVerificationToken=TMOppmiUc-huH5UOIQzatcp2nEzIkUvNSXEvL9HSVmPQzZahEp83oTD_hnqpCAW_dfOYCjR1X_fvXnLOra4p6b0vXgY1&ReferenceId=&ModelNumberHPPH=&ModelNumber2=&HighAirTemperatureHighHumidityHeatCapacity=&HighAirTemperatureHighHumidityHeatCapacity=&HighAirTemperatureHighHumidityCOP=&HighAirTemperatureHighHumidityCOP=&HighAirTemperatureMidHumidityHeatCapacity=&HighAirTemperatureMidHumidityHeatCapacity=&HighAirTemperatureMidHumidityCOP=&HighAirTemperatureMidHumidityCOP=&LowAirTemperatureMidHumidityHeatCapacity=&LowAirTemperatureMidHumidityHeatCapacity=&LowAirTemperatureMidHumidityCOP=&LowAirTemperatureMidHumidityCOP="
}

        data = json.dumps(payload)

        url = 'https://www.ahridirectory.org/NewSearch/Search'

        try:
            self.log.info(f'发送第{draw}页请求')
            response = session.post(url=url, data=data, timeout=300)
            #  若返回的响应状态码为200，则使用同一IP发送下一页请求；
            if response.status_code == 200:
                self.log.info(f'获取第{draw}页响应成功')
                page = response.json()
                return page
            # 若返回的响应状态码不为200，则需要更换IP，重新发送该页请求
            else:
                status_code = response.status_code
                self.log.error(f'获取第{draw}页响应失败，状态码{status_code}，更换IP，继续发送第{draw}页请求')
                # proxies = zhimaProxy().getIP()
                # session.proxies = proxies
                # 更换header
                self.get_page(draw, start, session)
        # 若请求异常，重新发送该页请求
        except RequestException as err:
            self.log.error(f'第{draw}页请求异常：{err}，继续发送第{draw}页请求')
            self.get_page(draw, start, session)

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
            item['Product Type'] = 'Heat Pump Pool Heater'
            item['AHRI Certified Reference Number'] = row[0]
            item['Old AHRI Reference Number'] = row[1]
            item['Model Status'] = row[2]
            item['Series Name'] = row[3]
            item['Brand Name'] = row[4]
            item['Model Number (For Split System - Air side or for Packaged Unit)'] = row[5]
            item['Model Number2 (For Split System Only - water side)'] = row[6]
            item['AHRI Type'] = row[7]
            item['Refrigerant Type'] = row[8]
            item['AHRI Certified Ratings - High Air Temperature High Humidity Heating Capacity, btuh'] = row[9]
            item['AHRI Certified Ratings - High Air Temperature High Humidity COP'] = row[10]
            item['AHRI Certified Ratings - High Air Temperature Mid Humidity Heating Capacity, btuh'] = row[11]
            item['AHRI Certified Ratings - High Air Temperature Mid Humidity COP'] = row[12]
            item['AHRI Certified Ratings - Low Air Temperature Mid Humidity Heating Capacity, btuhr'] = row[13]
            item['AHRI Certified Ratings - Low Air Temperature Mid Humidity COP'] = row[14]
            item['Volts'] = row[15]
            item['Phase'] = row[16]
            item['Hertz'] = row[17]
            item['Is Rerated'] = row[18]

            count = count + 1
            # 将每条产品记录储存到MongoDB中
            self.save_to_mongo(item)

        self.sum_saved_records(count)
        self.log.info(f'本页有{count}条结果')

    def main(self):
        session = requests.Session()
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Connection': 'keep-alive',
            'Content-Length': '2904',
            'Content-Type': 'application/json',
            'Cookie': 'ASP.NET_SessionId=ijsdb15begmvzkil3qt1arfi; __RequestVerificationToken=CyqFhfn-ocScYrOlIAnR2SDNoazAYfmn3uDRWUnL9S8UDGhA09gwCY7m2wZBY7JfE2XyxBzUSsTCjamDdRSkZCZrDGw1; usertypedescription=495; usertype=119.8.124.102; diranalytics=119.8.124.102/36',
            'Host': 'www.ahridirectory.org',
            'Origin': 'https://www.ahridirectory.org',
            'Proxy-Authorization': 'Basic aGVseTIyOmhvaG8xMjM0JCQ=',
            'Referer': 'https://www.ahridirectory.org/NewSearch?programId=36&searchTypeId=3',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0',
            'X-Requested-With': 'XMLHttpRequest'
        }

        session.headers.update(headers)
        first_page = self.get_page(1, 0, session)  # 返回字典格式
        records_filtered = first_page["recordsFiltered"]  # 获取条件过滤的产品数，产品不多，只有2页结果，无需更换header
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
            page = self.get_page(i, start, session)
            self.get_champ(page)
            self.log.info(f'有{page_num}页,完成第{i}页')
            time.sleep(5)

        self.log.info(f'爬取完成,已保存{self.sum}条结果')


if __name__ == '__main__':
    p = Ahri()
    p.main()