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
# 数据库登录
admin_db = client['admin']
user = 'admin'
pwd = 'admin888&&'
admin_db.authenticate(user, pwd)

db = client['eurovent']
product_collection = db['ahri_products']

'''
1. 发送get请求搜索页获取cookie，再发送post请求api
2. 获取首页response和搜索结果数，从而得到页数
3. 发送后面页数的request，若遇到状态码500，换ua，获取新cookie，继续爬取剩余页面
4. 将response储存到MongoDB
'''


# Residential - Air Conditioners and Air Conditioner Coils
# 由于产品数量过多，搜索结果过万页，搜索条目过百万，需要选择Outdoor Unit Brand Name 和 AHRI Type来缩小搜索范围
# brand: BRYANT HEATING AND COOLING SYSTEMS(6238), AHRI Type: 367, 有747页, 还要进一步限制范围爬取
class Ahri:

    def __init__(self):
        self.log = CommonLog().log()
        self.sum = 0
        self.session = HTMLSession()

    def get_session(self):
        # 发起get请求，获取新cookie
        a = self.session.get(
            url='https://www.ahridirectory.org/NewSearch?programId=68&searchTypeId=1&productTypeId=3500',
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
            'Referer': 'https://www.ahridirectory.org/NewSearch?programId=68&searchTypeId=1&productTypeId=3500',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': UserAgent().random,
            'X-Requested-With': 'XMLHttpRequest'
        }
        return headers

    def get_page(self, draw, start, brand_value, ac_ahri_type):
        """
        抓取索引页
        :param ac_ahri_type:
        :param brand_value:
        :param start:
        :param draw:
        """
        '''
        发送请求后，
        若返回的响应状态码为200，则使用同一session发送下一页请求；
        若返回的响应状态码不为200，则需要更换session，重新发送该页请求
        '''
        # Advanced Search填写的筛选条件为{Manufacturer Type: Systems, Outdoor Unit Brand Name: York, sold in?: USA & CANADA,
        #                               AHRI Type: SP-A, Ratings Calculated By: AHRI 210/240 - 2017 with Addendum 1}

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
                    "name": "manufacturertype",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 4,
                    "name": "AHRITypeUSAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 5,
                    "name": "CEEPhaseUSACHP",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 6,
                    "name": "SeriesNameUSAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 7,
                    "name": "OutdoorUnitBrandName",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 8,
                    "name": "ModelNumberUSAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 9,
                    "name": "IndoorUnitBrandName",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 10,
                    "name": "CoilModelNumberUSAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 11,
                    "name": "FurnaceModelNumberUSAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 12,
                    "name": "Capacity95FHighUSAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 13,
                    "name": "EER95FUSAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 14,
                    "name": "IEER",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 15,
                    "name": "SEERUSAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 16,
                    "name": "IndoorCoilAirQtyUSAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 17,
                    "name": "IndoorCoilAirQty2USAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 18,
                    "name": "IndoorCoilAirQty3USAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 19,
                    "name": "Capacity95FHighM1",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 20,
                    "name": "EER95FM1",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 21,
                    "name": "SEERM1",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 22,
                    "name": "TotalCoolingFullLoadAirVolumeRateM1",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 23,
                    "name": "IsHSVTCUSAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 24,
                    "name": "SoldInUSAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 25,
                    "name": "SearchRegion",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 26,
                    "name": "EstimatedCoolingCostUSAC",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 27,
                    "name": "EnergyGuideLabel",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 28,
                    "name": "FederalTaxCredit",
                    "searchable": True,
                    "orderable": True,
                    "search": {
                        "value": "",
                        "regex": False
                    }
                },
                {
                    "data": 29,
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
            "formData": f"hdnProgramId=68&hdnSearchTypeId=1&productType_id=3500"
                        f"&__RequestVerificationToken=-1sFYxiS_J5OlyMSujsozVXAUnYmaTYem7qSmb8Cx-s85ne-gpGfQIcryv4VFgXDfEQHm9Bd635uPeXP4JRMIByb2F41"
                        f"&ReferenceId=&ModelStatusId=&manufacturertype=33"
                        f"&AHRITypeUSAC={ac_ahri_type}&CEEPhaseUSACHP=&SeriesNameUSAC="
                        f"&OutdoorUnitBrandName={brand_value}&selectItemOutdoorUnitBrandName={brand_value}"
                        f"&ModelNumberUSAC=&CoilModelNumberUSAC=&FurnaceModelNumberUSAC=&FederalTaxCredit=&IsHSVTCUSAC="
                        f"&SoldInUSAC=1446&SoldInUSAC=1445&selectItemSoldInUSAC=1446&selectItemSoldInUSAC=1445"
                        f"&SearchRegion=&IsRerated=&MM1Switch=m"
                        f"&Capacity95FHighUSAC=&Capacity95FHighUSAC="
                        f"&SEERUSAC=&SEERUSAC=&EER95FUSAC=&EER95FUSAC=&IEER=&IEER="
        }

        data = json.dumps(payload)

        url = 'https://www.ahridirectory.org/NewSearch/Search'

        try:
            self.log.info(f'brand: {brand_value}, AHRI Type: {ac_ahri_type}, 发送第{draw}页请求')
            response = self.session.post(url=url, data=data, timeout=300)
            #  若返回的响应状态码为200，则使用同一session发送下一页请求；
            if response.status_code == 200:
                self.log.info(f'获取第{draw}页响应成功')
                try:
                    page = response.json()
                except json.decoder.JSONDecodeError as err:
                    self.log.error(f'第{draw}页响应成功，但json解析错误：{err}，继续发送第{draw}页请求')
                    # 更换cookie和ua
                    self.get_session()
                    self.session.headers.update(self.get_headers())
                    time.sleep(random.randint(5, 10))
                    self.get_page(draw, start, brand_value, ac_ahri_type)
                else:
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
                self.get_page(draw, start, brand_value, ac_ahri_type)
        # 若请求异常，更换cookie和ua，重新发送该页请求
        # 目前所见的错误有：('Connection aborted.', ConnectionResetError(10054, '远程主机强迫关闭了一个现有的连接。', None, 10054, None))
        except Exception as err:
            self.log.error(f'第{draw}页请求异常：{err}，继续发送第{draw}页请求')
            # 更换cookie和ua
            self.get_session()
            self.session.headers.update(self.get_headers())
            time.sleep(random.randint(5, 10))
            self.get_page(draw, start, brand_value, ac_ahri_type)

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
        if page is not None:
            rows = page["dataRaw"]
            count = 0
            for row in rows:
                item = {}
                item['Product Type'] = 'Air Conditioners and Air Conditioner Coils'
                item['AHRI Certified Reference Number'] = row[0]
                item['Old AHRI Reference Number'] = row[1]
                item['Model Status'] = row[2]
                item['Manufacturer Type'] = row[3]
                item['AHRI Type'] = row[4]
                item['Phase'] = row[5]
                item['Series Name'] = row[6]
                item['Outdoor Unit - Brand Name'] = row[7]
                item['Outdoor Unit - Model Number (Condenser or Single Package)'] = row[8]
                item['Indoor Unit - Brand Name'] = row[9]
                item['Indoor Unit - Model Number (Evaporator and/or Air Handler)'] = row[10]
                item['Furnace - Model Number'] = row[11]
                item[
                    'AHRI Certifed Ratings - AHRI 210/240 - 2017 - Cooling Capacity (A2) - Single or High Stage (95F),btuh'] = \
                row[12]
                item['AHRI Certifed Ratings - AHRI 210/240 - 2017 - EER (A2) - Single or High Stage (95F)'] = row[13]
                item['AHRI Certifed Ratings - AHRI 210/240 - 2017 - IEER'] = row[14]
                item['AHRI Certifed Ratings - AHRI 210/240 - 2017 - SEER'] = row[15]
                item['Indoor Full-Load Air Volume Rate (A2 SCFM)'] = row[16]
                item['Indoor Cooling Intermediate Air Volume Rate (Ev SCFM)'] = row[17]
                item['Indoor Cooling Minimum Air Volume Rate (B1 SCFM)'] = row[18]
                item[
                    'AHRI Certifed Ratings - AHRI 210/240 - 2023 - Cooling Capacity (AFull) - Single or High Stage (95F),btuh'] = \
                row[19]
                item['AHRI Certifed Ratings - AHRI 210/240 - 2023 - EER2 (AFull) - Single or High Stage (95F)'] = row[
                    20]
                item['AHRI Certifed Ratings - AHRI 210/240 - 2023 - SEER2'] = row[21]
                item['Total Cooling Full Load Air Volume Rate (SCFM)'] = row[22]
                item['Designated Tested Combination'] = row[23]
                item['Sold in?'] = row[24]
                item['Region'] = row[25]
                item['Estimated National Average Operating Cooling Cost ($)'] = row[26]
                item['Energy Guide Label'] = row[27]
                item['Eligible for Federal Tax Credit'] = row[28]
                item['Is Rerated'] = row[29]

                count = count + 1
                # 将每条产品记录储存到MongoDB中
                self.save_to_mongo(item)

            self.sum_saved_records(count)
            self.log.info(f'本页有{count}条结果')

    def main(self):
        # 生成第一次get请求，获取cookie，更新headers
        self.get_session()
        self.session.headers.update(self.get_headers())

        # AHRI Type编码：SPA, RCU-A-C, RCU-A-CB
        ac_ahri_type_list = [380, 366, 367]
        # 导入json文件，获取outdoor unit brand name
        input_filename = '../ac_brand.json'
        with open(input_filename) as f:
            brand_list = json.load(f)

        for ac_ahri_type in ac_ahri_type_list:
            for brand in brand_list['list']:
                first_page = self.get_page(1, 0, brand["brand_value"], ac_ahri_type)  # 返回字典格式
                while first_page is None:
                    first_page = self.get_page(1, 0, brand["brand_value"], ac_ahri_type)
                records_filtered = first_page["recordsFiltered"]
                if records_filtered != 0:
                    self.log.info(
                        f'brand: {brand["brand_name"]}({brand["brand_value"]}), AHRI Type: {ac_ahri_type}, 有{records_filtered}条结果')
                    self.get_champ(first_page)  # 爬取第一页
                    # 获取产品页面数
                    if records_filtered % 250 == 0:
                        page_num = records_filtered // 250
                    else:
                        page_num = (records_filtered // 250) + 1
                    self.log.info(
                        f'brand: {brand["brand_name"]}({brand["brand_value"]}), AHRI Type: {ac_ahri_type}, 有{page_num}页,完成第1页')

                    # 循环抓取产品每一页
                    for i in range(2, page_num + 1):
                        start = (i - 1) * 250
                        page = self.get_page(i, start, brand["brand_value"], ac_ahri_type)
                        while page is None:
                            self.log.info(
                                f'brand: {brand["brand_name"]}({brand["brand_value"]}), AHRI Type: {ac_ahri_type}第{i}页返回空白结果, 再次请求')
                            page = self.get_page(i, start, brand["brand_value"], ac_ahri_type)
                        if page is not None:
                            self.get_champ(page)
                            self.log.info(
                                f'brand: {brand["brand_name"]}({brand["brand_value"]}), AHRI Type: {ac_ahri_type}, 有{page_num}页,完成第{i}页')
                        time.sleep(random.randint(5, 10))

                    self.log.info(
                        f'brand: {brand["brand_name"]}({brand["brand_value"]}), AHRI Type: {ac_ahri_type}爬取完成,已保存{self.sum}条结果')
                else:
                    self.log.info(f'brand: {brand["brand_name"]}({brand["brand_value"]}), AHRI Type: {ac_ahri_type}无结果')
                time.sleep(random.randint(5, 10))
                self.sum = 0
            self.log.info(f'AHRI Type: {ac_ahri_type}爬取完成')


if __name__ == '__main__':
    p = Ahri()
    p.main()
