import pymongo
import pandas as pd
import datetime

client = pymongo.MongoClient(host='localhost', port=27017)
# 数据库登录
admin_db = client['admin']
user = 'admin'
pwd = 'admin888&&'
admin_db.authenticate(user, pwd)

db = client['eurovent']
product_collection = db['ahri_products']

# 本月
this_month = datetime.datetime.now().month

# result = product_collection.find({"Product Type":"Variable-Speed Mini-Split and Multi-Split Heat Pumps", "crawl_date":"2022-02-21"})
# data = pd.DataFrame(list(result))
# data.to_csv('D:\\MyData\\2.数据分析\\7.SMART共享站点\\5.行业上新\\2.北美\\2022\\vsmshp.csv', encoding='utf-8')

# result = product_collection.find({"Product Type":"Residential Furnaces", "crawl_date":"2022-02-21"})
# data = pd.DataFrame(list(result))
# data.to_csv('D:\\MyData\\2.数据分析\\7.SMART共享站点\\5.行业上新\\2.北美\\2022\\furnaces.csv', encoding='utf-8')

product_list = ["Variable-Speed Mini-Split and Multi-Split Heat Pumps", "Residential Furnaces",
                "Heat Pumps and Heat Pump Coils", "Air Conditioners and Air Conditioner Coils"]
for i in product_list:
    result = product_collection.find({"Product Type": i})
    data = pd.DataFrame(list(result))
    data.to_csv(f'D:\\MyData\\2.数据分析\\7.SMART共享站点\\5.行业上新\\2.北美\\2022\\{this_month}月\\{i}.csv', encoding='utf-8')