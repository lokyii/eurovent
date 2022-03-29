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
product_collection.create_index([('product_id', 1), ('crawl_date', 1)], unique=True,
                                     background=True)

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
            response = requests.post(url = result_url, data = data, headers=headers)
            if response.status_code == 200:
                page = response.json()
                return page
        except RequestException:
            return None

    def save_to_mongo(self, product):
        try:
            product_collection.insert_one(product)
        except DuplicateKeyError:
            print(f'{self.product_family}-{self.product_type}-{product["product_id"]}-{product["brand"]}-{product["model"]}，爬取日期{product["crawl_date"]}重复')

    def get_champ(self, page):
        """
        提取产品技术参数
        :param page:
        :return:
        """
        for item in page['rows']:
            product = {}
            product['product_id'] = item.get('id')
            crawl_date = time.strftime("%Y-%m-%d", time.localtime())
            product['crawl_date'] = crawl_date
            product['product_family'] = self.product_family
            product['product_family_text'] = self.product_family_text
            product['product_type'] = self.product_type
            product['product_type_text'] = self.product_type_text
            product['range'] = item.get('range')
            product['brand'] = item.get('brand')
            l = pq(item.get('model_name')).text().replace(u'\xa0', u' ').replace(' ', '').split()
            product['model_name'] = l[0]
            product['deleted_or_new'] = l[1] if len(l) == 2 else '-'
            if self.product_family == 'VRF':
                product[dict['champ_20']] = pq(item.get('champ_20')).text()
                product[dict['champ_21']] = pq(item.get('champ_21')).text()
                product[dict['champ_22']] = pq(item.get('champ_22')).text()
                product[dict['champ_23']] = pq(item.get('champ_23')).text()
                product[dict['champ_24']] = pq(item.get('champ_24')).text()
                product[dict['champ_25']] = pq(item.get('champ_25')).text()
                product[dict['champ_26']] = pq(item.get('champ_26')).text()
                product[dict['champ_29']] = pq(item.get('champ_29')).text()
                product[dict['champ_34']] = pq(item.get('champ_34')).text()
                product[dict['champ_35']] = pq(item.get('champ_35')).text()
                product[dict['champ_36']] = pq(item.get('champ_36')).text()
                product[dict['champ_37']] = pq(item.get('champ_37')).text()
                product[dict['champ_38']] = pq(item.get('champ_38')).text()
                product[dict['champ_39']] = pq(item.get('champ_39')).text()
                product[dict['champ_40']] = pq(item.get('champ_40')).text()
                product[dict['champ_41']] = pq(item.get('champ_41')).text()
                product[dict['champ_42']] = pq(item.get('champ_42')).text()
                product[dict['champ_43']] = pq(item.get('champ_43')).text()
                product[dict['champ_44']] = pq(item.get('champ_44')).text()
                product[dict['champ_45']] = pq(item.get('champ_45')).text()
                product[dict['champ_46']] = pq(item.get('champ_46')).text()
                product[dict['champ_47']] = pq(item.get('champ_47')).text()
                product[dict['champ_48']] = pq(item.get('champ_48')).text()
                product[dict['champ_49']] = pq(item.get('champ_49')).text()
                product[dict['champ_50']] = pq(item.get('champ_50')).text()
                product[dict['champ_51']] = pq(item.get('champ_51')).text()
                product[dict['champ_52']] = pq(item.get('champ_52')).text()
                product[dict['champ_53']] = pq(item.get('champ_53')).text()
                product[dict['champ_54']] = pq(item.get('champ_54')).text()
                product[dict['champ_55']] = pq(item.get('champ_55')).text()
                product[dict['champ_88']] = pq(item.get('champ_88')).text()
                product[dict['champ_92']] = pq(item.get('champ_92')).text()
                product[dict['champ_100']] = pq(item.get('champ_100')).text()
                product[dict['champ_102']] = pq(item.get('champ_102')).text()
                product[dict['champ_103']] = pq(item.get('champ_103')).text()
                product[dict['champ_104']] = pq(item.get('champ_104')).text()
                product[dict['champ_105']] = pq(item.get('champ_105')).text()
                product[dict['champ_106']] = pq(item.get('champ_106')).text()

            elif self.product_family == 'Eurovent-HP' and self.product_type == 'HP/ELEC/H/A/W':
                product[dict['champ_21']] = pq(item.get('champ_21')).text()
                product[dict['champ_22']] = pq(item.get('champ_22')).text()
                product[dict['champ_23']] = pq(item.get('champ_23')).text()

                product[dict['champ_320']] = pq(item.get('champ_320')).text()
                product[dict['champ_563']] = pq(item.get('champ_563')).text()
                product[dict['champ_26']] = pq(item.get('champ_26')).text()
                product[dict['champ_29']] = pq(item.get('champ_29')).text()
                product[dict['champ_34']] = pq(item.get('champ_34')).text()
                product[dict['champ_35']] = pq(item.get('champ_35')).text()
                product[dict['champ_36']] = pq(item.get('champ_36')).text()
                product[dict['champ_37']] = pq(item.get('champ_37')).text()
                product[dict['champ_38']] = pq(item.get('champ_38')).text()
                product[dict['champ_39']] = pq(item.get('champ_39')).text()
                product[dict['champ_40']] = pq(item.get('champ_40')).text()
                product[dict['champ_41']] = pq(item.get('champ_41')).text()
                product[dict['champ_42']] = pq(item.get('champ_42')).text()
                product[dict['champ_43']] = pq(item.get('champ_43')).text()
                product[dict['champ_44']] = pq(item.get('champ_44')).text()
                product[dict['champ_45']] = pq(item.get('champ_45')).text()
                product[dict['champ_46']] = pq(item.get('champ_46')).text()
                product[dict['champ_47']] = pq(item.get('champ_47')).text()
                product[dict['champ_48']] = pq(item.get('champ_48')).text()
                product[dict['champ_49']] = pq(item.get('champ_49')).text()
                product[dict['champ_50']] = pq(item.get('champ_50')).text()
                product[dict['champ_51']] = pq(item.get('champ_51')).text()
                product[dict['champ_52']] = pq(item.get('champ_52')).text()
                product[dict['champ_53']] = pq(item.get('champ_53')).text()
                product[dict['champ_54']] = pq(item.get('champ_54')).text()
                product[dict['champ_55']] = pq(item.get('champ_55')).text()
                product[dict['champ_88']] = pq(item.get('champ_88')).text()
                product[dict['champ_92']] = pq(item.get('champ_92')).text()
                product[dict['champ_100']] = pq(item.get('champ_100')).text()
                product[dict['champ_102']] = pq(item.get('champ_102')).text()
                product[dict['champ_103']] = pq(item.get('champ_103')).text()
                product[dict['champ_104']] = pq(item.get('champ_104')).text()
                product[dict['champ_105']] = pq(item.get('champ_105')).text()
                product[dict['champ_106']] = pq(item.get('champ_106')).text()

            elif self.product_family == 'Eurovent-HP' and self.product_type == 'HP/ELEC/R/A/W':


            elif self.product_family == 'Eurovent-HP' and self.product_type == 'HP/MP/A/4P/HR':
                product[dict['champ_81']] = pq(item.get('champ_81')).text()
                product[dict['champ_90']] = pq(item.get('champ_90')).text()
                product[dict['champ_91']] = pq(item.get('champ_91')).text()
                product[dict['champ_92']] = pq(item.get('champ_92')).text()
                product[dict['champ_93']] = pq(item.get('champ_93')).text()
                product[dict['champ_123']] = pq(item.get('champ_123')).text()
                product[dict['champ_124']] = pq(item.get('champ_124')).text()
                product[dict['champ_125']] = pq(item.get('champ_125')).text()
                product[dict['champ_131']] = pq(item.get('champ_131')).text()
                product[dict['champ_132']] = pq(item.get('champ_132')).text()
                product[dict['champ_133']] = pq(item.get('champ_133')).text()
                product[dict['champ_134']] = pq(item.get('champ_134')).text()
                product[dict['champ_307']] = pq(item.get('champ_307')).text()
                product[dict['champ_308']] = pq(item.get('champ_308')).text()
                product[dict['champ_309']] = pq(item.get('champ_309')).text()
                product[dict['champ_311']] = pq(item.get('champ_311')).text()
                product[dict['champ_1160']] = pq(item.get('champ_1160')).text()
                product[dict['champ_1161']] = pq(item.get('champ_1161')).text()
                product[dict['champ_1162']] = pq(item.get('champ_1162')).text()
                product[dict['champ_1163']] = pq(item.get('champ_1163')).text()

            elif self.product_family == 'Eurovent-HP' and (self.product_type == 'HP/LCP-HP/A/P/H' or self.product_type == 'HP/LCP/A/P/R'):
                product[dict['champ_81']] = pq(item.get('champ_81')).text()
                product[dict['champ_116']] = pq(item.get('champ_116')).text()
                product[dict['champ_117']] = pq(item.get('champ_117')).text()
                product[dict['champ_118']] = pq(item.get('champ_118')).text()
                product[dict['champ_119']] = pq(item.get('champ_119')).text()
                product[dict['champ_121']] = pq(item.get('champ_121')).text()
                product[dict['champ_126']] = pq(item.get('champ_126')).text()
                product[dict['champ_127']] = pq(item.get('champ_127')).text()
                product[dict['champ_128']] = pq(item.get('champ_128')).text()
                product[dict['champ_129']] = pq(item.get('champ_129')).text()
                product[dict['champ_130']] = pq(item.get('champ_130')).text()
                product[dict['champ_166']] = pq(item.get('champ_166')).text()
                product[dict['champ_167']] = pq(item.get('champ_167')).text()
                product[dict['champ_168']] = pq(item.get('champ_168')).text()
                product[dict['champ_169']] = pq(item.get('champ_169')).text()
                product[dict['champ_171']] = pq(item.get('champ_171')).text()
                product[dict['champ_173']] = pq(item.get('champ_173')).text()
                product[dict['champ_174']] = pq(item.get('champ_174')).text()
                product[dict['champ_175']] = pq(item.get('champ_175')).text()
                product[dict['champ_176']] = pq(item.get('champ_176')).text()
                product[dict['champ_177']] = pq(item.get('champ_177')).text()
                product[dict['champ_178']] = pq(item.get('champ_178')).text()
                product[dict['champ_179']] = pq(item.get('champ_179')).text()
                product[dict['champ_180']] = pq(item.get('champ_180')).text()
                product[dict['champ_181']] = pq(item.get('champ_181')).text()
                product[dict['champ_183']] = pq(item.get('champ_183')).text()
                product[dict['champ_185']] = pq(item.get('champ_185')).text()
                product[dict['champ_186']] = pq(item.get('champ_186')).text()
                product[dict['champ_187']] = pq(item.get('champ_187')).text()
                product[dict['champ_188']] = pq(item.get('champ_188')).text()
                product[dict['champ_190']] = pq(item.get('champ_190')).text()
                product[dict['champ_192']] = pq(item.get('champ_192')).text()
                product[dict['champ_193']] = pq(item.get('champ_193')).text()
                product[dict['champ_194']] = pq(item.get('champ_194')).text()
                product[dict['champ_195']] = pq(item.get('champ_195')).text()
                product[dict['champ_196']] = pq(item.get('champ_196')).text()
                product[dict['champ_250']] = pq(item.get('champ_250')).text()
                product[dict['champ_257']] = pq(item.get('champ_257')).text()
                product[dict['champ_265']] = pq(item.get('champ_265')).text()
                product[dict['champ_266']] = pq(item.get('champ_266')).text()
                product[dict['champ_267']] = pq(item.get('champ_267')).text()
                product[dict['champ_268']] = pq(item.get('champ_268')).text()
                product[dict['champ_306']] = pq(item.get('champ_306')).text()
                product[dict['champ_307']] = pq(item.get('champ_307')).text()
                product[dict['champ_308']] = pq(item.get('champ_308')).text()
                product[dict['champ_309']] = pq(item.get('champ_309')).text()
                product[dict['champ_311']] = pq(item.get('champ_311')).text()
                product[dict['champ_313']] = pq(item.get('champ_313')).text()
                product[dict['champ_314']] = pq(item.get('champ_314')).text()
                product[dict['champ_317']] = pq(item.get('champ_317')).text()
                product[dict['champ_1160']] = pq(item.get('champ_1160')).text()
                product[dict['champ_1161']] = pq(item.get('champ_1161')).text()
                product[dict['champ_1163']] = pq(item.get('champ_1163')).text()

            elif self.product_family == 'Eurovent-HP' and (self.product_type == 'HP/ELEC/PO/H/A/W' or self.product_type == 'HP/LCP-HP/A/S/H'
                                                           or self.product_type == 'HP/LCP/A/S/R'):
                product[dict['champ_23']] = pq(item.get('champ_23')).text()
                product[dict['champ_318']] = pq(item.get('champ_318')).text()
                product[dict['champ_319']] = pq(item.get('champ_319')).text()
                product[dict['champ_1006']] = pq(item.get('champ_1006')).text()
                product[dict['champ_1007']] = pq(item.get('champ_1007')).text()
                product[dict['champ_1008']] = pq(item.get('champ_1008')).text()
                product[dict['champ_1009']] = pq(item.get('champ_1009')).text()
                product[dict['champ_1010']] = pq(item.get('champ_1010')).text()
                product[dict['champ_1011']] = pq(item.get('champ_1011')).text()
                product[dict['champ_1012']] = pq(item.get('champ_1012')).text()
                product[dict['champ_1013']] = pq(item.get('champ_1013')).text()
                product[dict['champ_1014']] = pq(item.get('champ_1014')).text()
                product[dict['champ_1015']] = pq(item.get('champ_1015')).text()
                product[dict['champ_1016']] = pq(item.get('champ_1016')).text()
                product[dict['champ_1017']] = pq(item.get('champ_1017')).text()
                product[dict['champ_1018']] = pq(item.get('champ_1018')).text()
                product[dict['champ_1019']] = pq(item.get('champ_1019')).text()
                product[dict['champ_1020']] = pq(item.get('champ_1020')).text()
                product[dict['champ_1100']] = pq(item.get('champ_1100')).text()
                product[dict['champ_1101']] = pq(item.get('champ_1101')).text()
                product[dict['champ_1102']] = pq(item.get('champ_1102')).text()
                product[dict['champ_1103']] = pq(item.get('champ_1103')).text()
                product[dict['champ_1104']] = pq(item.get('champ_1104')).text()
                product[dict['champ_1105']] = pq(item.get('champ_1105')).text()
                product[dict['champ_1106']] = pq(item.get('champ_1106')).text()
                product[dict['champ_1107']] = pq(item.get('champ_1107')).text()
                product[dict['champ_1110']] = pq(item.get('champ_1110')).text()

            elif self.product_family == 'MCS007':
                product[dict['champ_20']] = pq(item.get('champ_20')).text()
                product[dict['champ_21']] = pq(item.get('champ_21')).text()
                product[dict['champ_22']] = pq(item.get('champ_22')).text()
                product[dict['champ_23']] = pq(item.get('champ_23')).text()
                product[dict['champ_24']] = pq(item.get('champ_24')).text()
                product[dict['champ_25']] = pq(item.get('champ_25')).text()
                product[dict['champ_26']] = pq(item.get('champ_26')).text()
                product[dict['champ_27']] = pq(item.get('champ_27')).text()
                product[dict['champ_28']] = pq(item.get('champ_28')).text()
                product[dict['champ_29']] = pq(item.get('champ_29')).text()
                product[dict['champ_30']] = pq(item.get('champ_30')).text()
                product[dict['champ_31']] = pq(item.get('champ_31')).text()
                product[dict['champ_32']] = pq(item.get('champ_32')).text()
                product[dict['champ_33']] = pq(item.get('champ_33')).text()
                product[dict['champ_34']] = pq(item.get('champ_34')).text()
                product[dict['champ_35']] = pq(item.get('champ_35')).text()
                product[dict['champ_36']] = pq(item.get('champ_36')).text()
                product[dict['champ_37']] = pq(item.get('champ_37')).text()
                product[dict['champ_38']] = pq(item.get('champ_38')).text()
                product[dict['champ_39']] = pq(item.get('champ_39')).text()
                product[dict['champ_40']] = pq(item.get('champ_40')).text()
                product[dict['champ_82']] = pq(item.get('champ_82')).text()
                product[dict['champ_83']] = pq(item.get('champ_83')).text()
                product[dict['champ_84']] = pq(item.get('champ_84')).text()
                product[dict['champ_85']] = pq(item.get('champ_85')).text()
                product[dict['champ_86']] = pq(item.get('champ_86')).text()
                product[dict['champ_87']] = pq(item.get('champ_87')).text()
                product[dict['champ_88']] = pq(item.get('champ_88')).text()
                product[dict['champ_89']] = pq(item.get('champ_89')).text()

            elif self.product_family == 'NF414' and self.product_type == 'NF414/ELEC/SM/H/A-W':
                product[dict['champ_20']] = pq(item.get('champ_20')).text()
                product[dict['champ_21']] = pq(item.get('champ_21')).text()
                product[dict['champ_22']] = pq(item.get('champ_22')).text()
                product[dict['champ_33']] = pq(item.get('champ_33')).text()
                product[dict['champ_34']] = pq(item.get('champ_34')).text()
                product[dict['champ_35']] = pq(item.get('champ_35')).text()
                product[dict['champ_36']] = pq(item.get('champ_36')).text()
                product[dict['champ_43']] = pq(item.get('champ_43')).text()
                product[dict['champ_44']] = pq(item.get('champ_44')).text()
                product[dict['champ_45']] = pq(item.get('champ_45')).text()
                product[dict['champ_46']] = pq(item.get('champ_46')).text()
                product[dict['champ_69']] = pq(item.get('champ_69')).text()
                product[dict['champ_70']] = pq(item.get('champ_70')).text()
                product[dict['champ_71']] = pq(item.get('champ_71')).text()
                product[dict['champ_72']] = pq(item.get('champ_72')).text()
                product[dict['champ_73']] = pq(item.get('champ_73')).text()
                product[dict['champ_74']] = pq(item.get('champ_74')).text()
                product[dict['champ_75']] = pq(item.get('champ_75')).text()
                product[dict['champ_76']] = pq(item.get('champ_76')).text()
                product[dict['champ_77']] = pq(item.get('champ_77')).text()
                product[dict['champ_84']] = pq(item.get('champ_84')).text()
                product[dict['champ_85']] = pq(item.get('champ_85')).text()
                product[dict['champ_86']] = pq(item.get('champ_86')).text()
                product[dict['champ_87']] = pq(item.get('champ_87')).text()
                product[dict['champ_88']] = pq(item.get('champ_88')).text()
                product[dict['champ_89']] = pq(item.get('champ_89')).text()
                product[dict['champ_90']] = pq(item.get('champ_90')).text()
                product[dict['champ_91']] = pq(item.get('champ_91')).text()
                product[dict['champ_92']] = pq(item.get('champ_92')).text()
                product[dict['champ_99']] = pq(item.get('champ_99')).text()
                product[dict['champ_100']] = pq(item.get('champ_100')).text()
                product[dict['champ_101']] = pq(item.get('champ_101')).text()
                product[dict['champ_105']] = pq(item.get('champ_105')).text()
                product[dict['champ_106']] = pq(item.get('champ_106')).text()
                product[dict['champ_107']] = pq(item.get('champ_107')).text()
                product[dict['champ_526']] = pq(item.get('champ_526')).text()
                product[dict['champ_527']] = pq(item.get('champ_527')).text()
                product[dict['champ_529']] = pq(item.get('champ_529')).text()
                product[dict['champ_531']] = pq(item.get('champ_531')).text()
                product[dict['champ_533']] = pq(item.get('champ_533')).text()
                product[dict['champ_534']] = pq(item.get('champ_534')).text()
                product[dict['champ_535']] = pq(item.get('champ_535')).text()
                product[dict['champ_536']] = pq(item.get('champ_536')).text()
                product[dict['champ_539']] = pq(item.get('champ_539')).text()
                product[dict['champ_541']] = pq(item.get('champ_541')).text()
                product[dict['champ_543']] = pq(item.get('champ_543')).text()
                product[dict['champ_544']] = pq(item.get('champ_544')).text()
                product[dict['champ_546']] = pq(item.get('champ_546')).text()
                product[dict['champ_548']] = pq(item.get('champ_548')).text()
                product[dict['champ_550']] = pq(item.get('champ_550')).text()
                product[dict['champ_551']] = pq(item.get('champ_551')).text()
                product[dict['champ_556']] = pq(item.get('champ_556')).text()
                product[dict['champ_558']] = pq(item.get('champ_558')).text()
                product[dict['champ_560']] = pq(item.get('champ_560')).text()
                product[dict['champ_562']] = pq(item.get('champ_562')).text()
                product[dict['champ_563']] = pq(item.get('champ_563')).text()
                product[dict['champ_591']] = pq(item.get('champ_591')).text()
                product[dict['champ_592']] = pq(item.get('champ_592')).text()

            elif self.product_family == 'NF414' and self.product_type == 'NF414/ELEC/SM/R/A-W':
                product[dict['champ_20']] = pq(item.get('champ_20')).text()
                product[dict['champ_22']] = pq(item.get('champ_22')).text()
                product[dict['champ_33']] = pq(item.get('champ_33')).text()
                product[dict['champ_34']] = pq(item.get('champ_34')).text()
                product[dict['champ_35']] = pq(item.get('champ_35')).text()
                product[dict['champ_36']] = pq(item.get('champ_36')).text()
                product[dict['champ_43']] = pq(item.get('champ_43')).text()
                product[dict['champ_44']] = pq(item.get('champ_44')).text()
                product[dict['champ_45']] = pq(item.get('champ_45')).text()
                product[dict['champ_46']] = pq(item.get('champ_46')).text()
                product[dict['champ_47']] = pq(item.get('champ_47')).text()
                product[dict['champ_48']] = pq(item.get('champ_48')).text()
                product[dict['champ_69']] = pq(item.get('champ_69')).text()
                product[dict['champ_70']] = pq(item.get('champ_70')).text()
                product[dict['champ_71']] = pq(item.get('champ_71')).text()
                product[dict['champ_72']] = pq(item.get('champ_72')).text()
                product[dict['champ_73']] = pq(item.get('champ_73')).text()
                product[dict['champ_74']] = pq(item.get('champ_74')).text()
                product[dict['champ_75']] = pq(item.get('champ_75')).text()
                product[dict['champ_76']] = pq(item.get('champ_76')).text()
                product[dict['champ_77']] = pq(item.get('champ_77')).text()
                product[dict['champ_84']] = pq(item.get('champ_84')).text()
                product[dict['champ_85']] = pq(item.get('champ_85')).text()
                product[dict['champ_86']] = pq(item.get('champ_86')).text()
                product[dict['champ_87']] = pq(item.get('champ_87')).text()
                product[dict['champ_88']] = pq(item.get('champ_88')).text()
                product[dict['champ_89']] = pq(item.get('champ_89')).text()
                product[dict['champ_90']] = pq(item.get('champ_90')).text()
                product[dict['champ_91']] = pq(item.get('champ_91')).text()
                product[dict['champ_92']] = pq(item.get('champ_92')).text()
                product[dict['champ_99']] = pq(item.get('champ_99')).text()
                product[dict['champ_100']] = pq(item.get('champ_100')).text()
                product[dict['champ_101']] = pq(item.get('champ_101')).text()
                product[dict['champ_102']] = pq(item.get('champ_102')).text()
                product[dict['champ_103']] = pq(item.get('champ_103')).text()
                product[dict['champ_104']] = pq(item.get('champ_104')).text()
                product[dict['champ_105']] = pq(item.get('champ_105')).text()
                product[dict['champ_106']] = pq(item.get('champ_106')).text()
                product[dict['champ_107']] = pq(item.get('champ_107')).text()
                product[dict['champ_126']] = pq(item.get('champ_126')).text()
                product[dict['champ_127']] = pq(item.get('champ_127')).text()
                product[dict['champ_128']] = pq(item.get('champ_128')).text()
                product[dict['champ_129']] = pq(item.get('champ_129')).text()
                product[dict['champ_130']] = pq(item.get('champ_130')).text()
                product[dict['champ_131']] = pq(item.get('champ_131')).text()
                product[dict['champ_526']] = pq(item.get('champ_526')).text()
                product[dict['champ_527']] = pq(item.get('champ_527')).text()
                product[dict['champ_529']] = pq(item.get('champ_529')).text()
                product[dict['champ_531']] = pq(item.get('champ_531')).text()
                product[dict['champ_533']] = pq(item.get('champ_533')).text()
                product[dict['champ_536']] = pq(item.get('champ_536')).text()
                product[dict['champ_537']] = pq(item.get('champ_537')).text()
                product[dict['champ_539']] = pq(item.get('champ_539')).text()
                product[dict['champ_541']] = pq(item.get('champ_541')).text()
                product[dict['champ_543']] = pq(item.get('champ_543')).text()
                product[dict['champ_546']] = pq(item.get('champ_546')).text()
                product[dict['champ_548']] = pq(item.get('champ_548')).text()
                product[dict['champ_550']] = pq(item.get('champ_550')).text()
                product[dict['champ_551']] = pq(item.get('champ_551')).text()
                product[dict['champ_553']] = pq(item.get('champ_553')).text()
                product[dict['champ_554']] = pq(item.get('champ_554')).text()
                product[dict['champ_556']] = pq(item.get('champ_556')).text()
                product[dict['champ_558']] = pq(item.get('champ_558')).text()
                product[dict['champ_560']] = pq(item.get('champ_560')).text()
                product[dict['champ_562']] = pq(item.get('champ_562')).text()
                product[dict['champ_563']] = pq(item.get('champ_563')).text()
                product[dict['champ_591']] = pq(item.get('champ_591')).text()
                product[dict['champ_592']] = pq(item.get('champ_592')).text()

            elif self.product_family == 'NF414' and self.product_type == 'NF414/ELEC/DM/H/A-W':
                product[dict['champ_21']] = pq(item.get('champ_21')).text()
                product[dict['champ_22']] = pq(item.get('champ_22')).text()
                product[dict['champ_33']] = pq(item.get('champ_33')).text()
                product[dict['champ_34']] = pq(item.get('champ_34')).text()
                product[dict['champ_35']] = pq(item.get('champ_35')).text()
                product[dict['champ_36']] = pq(item.get('champ_36')).text()
                product[dict['champ_75']] = pq(item.get('champ_75')).text()
                product[dict['champ_76']] = pq(item.get('champ_76')).text()
                product[dict['champ_77']] = pq(item.get('champ_77')).text()
                product[dict['champ_526']] = pq(item.get('champ_526')).text()
                product[dict['champ_527']] = pq(item.get('champ_527')).text()
                product[dict['champ_529']] = pq(item.get('champ_529')).text()
                product[dict['champ_531']] = pq(item.get('champ_531')).text()
                product[dict['champ_533']] = pq(item.get('champ_533')).text()
                product[dict['champ_534']] = pq(item.get('champ_534')).text()
                product[dict['champ_535']] = pq(item.get('champ_535')).text()
                product[dict['champ_546']] = pq(item.get('champ_546')).text()
                product[dict['champ_548']] = pq(item.get('champ_548')).text()
                product[dict['champ_550']] = pq(item.get('champ_550')).text()
                product[dict['champ_551']] = pq(item.get('champ_551')).text()
                product[dict['champ_552']] = pq(item.get('champ_552')).text()
                product[dict['champ_556']] = pq(item.get('champ_556')).text()
                product[dict['champ_558']] = pq(item.get('champ_558')).text()
                product[dict['champ_560']] = pq(item.get('champ_560')).text()
                product[dict['champ_562']] = pq(item.get('champ_562')).text()
                product[dict['champ_563']] = pq(item.get('champ_563')).text()
                product[dict['champ_564']] = pq(item.get('champ_564')).text()
                product[dict['champ_565']] = pq(item.get('champ_565')).text()
                product[dict['champ_566']] = pq(item.get('champ_566')).text()
                product[dict['champ_567']] = pq(item.get('champ_567')).text()
                product[dict['champ_568']] = pq(item.get('champ_568')).text()
                product[dict['champ_569']] = pq(item.get('champ_569')).text()
                product[dict['champ_570']] = pq(item.get('champ_570')).text()
                product[dict['champ_571']] = pq(item.get('champ_571')).text()
                product[dict['champ_572']] = pq(item.get('champ_572')).text()
                product[dict['champ_573']] = pq(item.get('champ_573')).text()
                product[dict['champ_574']] = pq(item.get('champ_574')).text()
                product[dict['champ_575']] = pq(item.get('champ_575')).text()
                product[dict['champ_576']] = pq(item.get('champ_576')).text()
                product[dict['champ_580']] = pq(item.get('champ_580')).text()
                product[dict['champ_591']] = pq(item.get('champ_591')).text()
                product[dict['champ_592']] = pq(item.get('champ_592')).text()

            elif self.product_family == 'NF414' and self.product_type == 'NF414/ELEC/DM/R/A-W':
                product[dict['champ_20']] = pq(item.get('champ_20')).text()
                product[dict['champ_22']] = pq(item.get('champ_22')).text()
                product[dict['champ_43']] = pq(item.get('champ_43')).text()
                product[dict['champ_44']] = pq(item.get('champ_44')).text()
                product[dict['champ_45']] = pq(item.get('champ_45')).text()
                product[dict['champ_46']] = pq(item.get('champ_46')).text()
                product[dict['champ_47']] = pq(item.get('champ_47')).text()
                product[dict['champ_48']] = pq(item.get('champ_48')).text()
                product[dict['champ_69']] = pq(item.get('champ_69')).text()
                product[dict['champ_70']] = pq(item.get('champ_70')).text()
                product[dict['champ_71']] = pq(item.get('champ_71')).text()
                product[dict['champ_72']] = pq(item.get('champ_72')).text()
                product[dict['champ_73']] = pq(item.get('champ_73')).text()
                product[dict['champ_74']] = pq(item.get('champ_74')).text()
                product[dict['champ_75']] = pq(item.get('champ_75')).text()
                product[dict['champ_76']] = pq(item.get('champ_76')).text()
                product[dict['champ_77']] = pq(item.get('champ_77')).text()
                product[dict['champ_84']] = pq(item.get('champ_84')).text()
                product[dict['champ_85']] = pq(item.get('champ_85')).text()
                product[dict['champ_86']] = pq(item.get('champ_86')).text()
                product[dict['champ_87']] = pq(item.get('champ_87')).text()
                product[dict['champ_88']] = pq(item.get('champ_88')).text()
                product[dict['champ_89']] = pq(item.get('champ_89')).text()
                product[dict['champ_90']] = pq(item.get('champ_90')).text()
                product[dict['champ_91']] = pq(item.get('champ_91')).text()
                product[dict['champ_92']] = pq(item.get('champ_92')).text()
                product[dict['champ_99']] = pq(item.get('champ_99')).text()
                product[dict['champ_100']] = pq(item.get('champ_100')).text()
                product[dict['champ_101']] = pq(item.get('champ_101')).text()
                product[dict['champ_102']] = pq(item.get('champ_102')).text()
                product[dict['champ_103']] = pq(item.get('champ_103')).text()
                product[dict['champ_104']] = pq(item.get('champ_104')).text()
                product[dict['champ_105']] = pq(item.get('champ_105')).text()
                product[dict['champ_106']] = pq(item.get('champ_106')).text()
                product[dict['champ_107']] = pq(item.get('champ_107')).text()
                product[dict['champ_126']] = pq(item.get('champ_126')).text()
                product[dict['champ_127']] = pq(item.get('champ_127')).text()
                product[dict['champ_128']] = pq(item.get('champ_128')).text()
                product[dict['champ_129']] = pq(item.get('champ_129')).text()
                product[dict['champ_130']] = pq(item.get('champ_130')).text()
                product[dict['champ_131']] = pq(item.get('champ_131')).text()
                product[dict['champ_536']] = pq(item.get('champ_536')).text()
                product[dict['champ_537']] = pq(item.get('champ_537')).text()
                product[dict['champ_539']] = pq(item.get('champ_539')).text()
                product[dict['champ_541']] = pq(item.get('champ_541')).text()
                product[dict['champ_543']] = pq(item.get('champ_543')).text()
                product[dict['champ_546']] = pq(item.get('champ_546')).text()
                product[dict['champ_548']] = pq(item.get('champ_548')).text()
                product[dict['champ_550']] = pq(item.get('champ_550')).text()
                product[dict['champ_551']] = pq(item.get('champ_551')).text()
                product[dict['champ_552']] = pq(item.get('champ_552')).text()
                product[dict['champ_553']] = pq(item.get('champ_553')).text()
                product[dict['champ_554']] = pq(item.get('champ_554')).text()
                product[dict['champ_556']] = pq(item.get('champ_556')).text()
                product[dict['champ_558']] = pq(item.get('champ_558')).text()
                product[dict['champ_560']] = pq(item.get('champ_560')).text()
                product[dict['champ_562']] = pq(item.get('champ_562')).text()
                product[dict['champ_564']] = pq(item.get('champ_564')).text()
                product[dict['champ_565']] = pq(item.get('champ_565')).text()
                product[dict['champ_566']] = pq(item.get('champ_566')).text()
                product[dict['champ_567']] = pq(item.get('champ_567')).text()
                product[dict['champ_568']] = pq(item.get('champ_568')).text()
                product[dict['champ_569']] = pq(item.get('champ_569')).text()
                product[dict['champ_570']] = pq(item.get('champ_570')).text()
                product[dict['champ_571']] = pq(item.get('champ_571')).text()
                product[dict['champ_572']] = pq(item.get('champ_572')).text()
                product[dict['champ_573']] = pq(item.get('champ_573')).text()
                product[dict['champ_574']] = pq(item.get('champ_574')).text()
                product[dict['champ_575']] = pq(item.get('champ_575')).text()
                product[dict['champ_576']] = pq(item.get('champ_576')).text()
                product[dict['champ_577']] = pq(item.get('champ_577')).text()
                product[dict['champ_578']] = pq(item.get('champ_578')).text()
                product[dict['champ_579']] = pq(item.get('champ_579')).text()
                product[dict['champ_580']] = pq(item.get('champ_580')).text()
                product[dict['champ_581']] = pq(item.get('champ_581')).text()
                product[dict['champ_582']] = pq(item.get('champ_582')).text()
                product[dict['champ_583']] = pq(item.get('champ_583')).text()
                product[dict['champ_584']] = pq(item.get('champ_584')).text()
                product[dict['champ_585']] = pq(item.get('champ_585')).text()
                product[dict['champ_586']] = pq(item.get('champ_586')).text()
                product[dict['champ_587']] = pq(item.get('champ_587')).text()
                product[dict['champ_588']] = pq(item.get('champ_588')).text()
                product[dict['champ_591']] = pq(item.get('champ_591')).text()
                product[dict['champ_592']] = pq(item.get('champ_592')).text()

            elif self.product_family == 'NF414' and self.product_type == 'NF414/ELEC/PO/H/A-W':
                product[dict['champ_20']] = pq(item.get('champ_20')).text()
                product[dict['champ_511']] = pq(item.get('champ_511')).text()
                product[dict['champ_512']] = pq(item.get('champ_512')).text()
                product[dict['champ_513']] = pq(item.get('champ_513')).text()
                product[dict['champ_514']] = pq(item.get('champ_514')).text()
                product[dict['champ_515']] = pq(item.get('champ_515')).text()
                product[dict['champ_516']] = pq(item.get('champ_516')).text()
                product[dict['champ_517']] = pq(item.get('champ_517')).text()
                product[dict['champ_518']] = pq(item.get('champ_518')).text()
                product[dict['champ_519']] = pq(item.get('champ_519')).text()
                product[dict['champ_520']] = pq(item.get('champ_520')).text()
                product[dict['champ_521']] = pq(item.get('champ_521')).text()
                product[dict['champ_522']] = pq(item.get('champ_522')).text()
                product[dict['champ_523']] = pq(item.get('champ_523')).text()
                product[dict['champ_524']] = pq(item.get('champ_524')).text()
                product[dict['champ_525']] = pq(item.get('champ_525')).text()
                product[dict['champ_550']] = pq(item.get('champ_550')).text()
                product[dict['champ_551']] = pq(item.get('champ_551')).text()
                product[dict['champ_556']] = pq(item.get('champ_556')).text()
                product[dict['champ_558']] = pq(item.get('champ_558')).text()
                product[dict['champ_560']] = pq(item.get('champ_560')).text()
                product[dict['champ_562']] = pq(item.get('champ_562')).text()
                product[dict['champ_563']] = pq(item.get('champ_563')).text()
                product[dict['champ_591']] = pq(item.get('champ_591')).text()
                product[dict['champ_592']] = pq(item.get('champ_592')).text()

            self.save_to_mongo(product)


    def main(self):
        first_page = self.get_page("0")  # 返回字段格式
        records_filtered = first_page["total"]  # 获取条件过滤的产品数
        self.get_champ(first_page)  # 爬取第一页

        # 获取产品页面数
        if records_filtered > 15:
            if records_filtered % 15 == 0:
                page_num = records_filtered // 15
            else:
                page_num = (records_filtered // 15) + 1
            print(f'{self.product_family}（{self.product_family_text}）-{self.product_type}（{self.product_type_text}）有{records_filtered}条结果，有{page_num}页')
            print(f'正在爬取{self.product_family}（{self.product_family_text}）-{self.product_type}（{self.product_type_text}）， 共有{page_num}页， 完成第1页')

            # 循环抓取产品每一页
            for i in range(1, page_num):
                offset = i * 15
                page = self.get_page(offset)
                self.get_champ(page)
                print(f'正在爬取{self.product_family}（{self.product_family_text}）-{self.product_type}（{self.product_type_text}）， 共有{page_num}页， 完成第{i + 1}页')
            print(f'{self.product_family}-{self.product_type}爬取完成')

        else:
            print(f'{self.product_family}（{self.product_family_text}）-{self.product_type}（{self.product_type_text}），结果只有1页, 爬取完成')

class Products:

    def __init__(self):
        # 把各product_family的技术参数ID和值放到字典中
        self.vrf_dict = {}
        self.eurovent_hp_dict = {}
        self.msc007_dict = {}
        self.nf414_dict = {}
        for champ in champ_collection.find():
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






