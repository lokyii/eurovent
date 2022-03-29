import json
from fake_useragent import UserAgent
import requests
from lxml import etree

# url = 'https://www.ahridirectory.org/NewSearch?programId=68&searchTypeId=1&productTypeId=3500'  # ac
url = 'https://www.ahridirectory.org/NewSearch?programId=69&searchTypeId=1&productTypeId=3523'  # hp

response = requests.get(url, headers={'User-Agent': UserAgent().random})
# result_list = {"product_type": "Air Conditioners and Air Conditioner Coils", "list": []}  # ac
result_list = {"product_type": "Heat Pumps and Heat Pump Coils", "list": []}  # hp

html = etree.HTML(response.text)
brand_name_list = html.xpath("//*[@id='OutdoorUnitBrandName']/option/text()")
brand_value_list = html.xpath("//*[@id='OutdoorUnitBrandName']/option/@value")

for i in range(len(brand_name_list)):
    item = {}
    if "," in brand_value_list[i]:
        brand_value_list[i] = brand_value_list[i].replace(",", "%2C")
    item['brand_name'] = brand_name_list[i]
    item['brand_value'] = brand_value_list[i]
    result_list["list"].append(item)
    print(item)

print(result_list)

# 将字典保存到json文件中
json_str = json.dumps(result_list)
# ac
# with open("../ac_brand.json", "w") as f:
#     f.write(json_str)
# hp
with open("../hp_brand.json", "w") as f:
    f.write(json_str)

# 将json文件导入
# input_filename = '../ac_brand.json'
# with open(input_filename) as f:
#     input_json = json.load(f)
