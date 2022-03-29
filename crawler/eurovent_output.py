import pymongo
import pandas as pd

client = pymongo.MongoClient(host='localhost', port=27017)
# 数据库登录
admin_db = client['admin']
user = 'admin'
pwd = 'admin888&&'
admin_db.authenticate(user, pwd)

db = client['eurovent']
product_collection = db['eurovent_products']
product_backup_collection = db['eurovent_vrf_20220301']

# result = product_collection.find({"product_family_abbr": "VRF"})
# for i in result:
#     product_backup_collection.insert_one(i)
product_collection.delete_many({"product_family_abbr": "VRF"})

print("finished!")