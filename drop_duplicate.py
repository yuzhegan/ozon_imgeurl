# encoding='utf-8

# @Time: 2024-04-12
# @File: %
#!/usr/bin/env
from icecream import ic
import os

from pymongo import MongoClient

# 连接到MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ozon"]
collection = db["ozon_product"]

# 找到重复的ID，并且对每个重复的ID，获取除第一个文档之外的所有文档的_id
pipeline = [
    {"$group": {
        "_id": "$ID",  # 或者使用 "_id": "$_id"，取决于你的ID字段名
        "uniqueIds": {"$addToSet": "$_id"},
        "count": {"$sum": 1}
    }},
    {"$match": {
        "count": {"$gt": 1}
    }},
    {"$project": {
        "documentIds": {"$slice": ["$uniqueIds", 1, {"$subtract": ["$count", 1]}]}
    }}
]

duplicate_documents = list(collection.aggregate(pipeline))

# 打印出重复文档的ID，用于后续的删除操作

for doc in duplicate_documents:
    for doc_id in doc["documentIds"]:
        print(doc["documentIds"])
        collection.delete_one({"_id": doc_id})
