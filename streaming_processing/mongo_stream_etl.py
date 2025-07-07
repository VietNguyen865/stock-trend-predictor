from pymongo import MongoClient
import pandas as pd

# Kết nối đến MongoDB replica set
client = MongoClient("mongodb://192.168.1.148:27017/?replicaSet=rs0&directConnection=false&serverSelectionTimeoutMS=5000")
db = client['stock_db']
src_collection = db['stock_data']

# Collection đích
history_collection = db['stock_history_data']
realtime_collection = db['stock_realtime_data']

# Stream pipeline
pipeline = [{'$match': {}}]
change_stream = src_collection.watch(pipeline, full_document='updateLookup')

print("📡 Đang theo dõi dữ liệu từ MongoDB...")
for change in change_stream:
    full_doc = change.get("fullDocument")
    if not full_doc:
        continue

    source = full_doc.get("source", "unknown")
    record = {
        "open": full_doc["open"],
        "high": full_doc["high"],
        "low": full_doc["low"],
        "close": full_doc["close"],
        "volume": full_doc["volume"],
        "timestamp": full_doc.get("time")  # Nếu có trường thời gian
    }

    if source == "history":
        history_collection.insert_one(record)
        print("📥 Ghi vào collection: stock_history_data")
    else:
        realtime_collection.insert_one(record)
        print("📈 Ghi vào collection: stock_realtime_data")
