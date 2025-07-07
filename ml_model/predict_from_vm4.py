from pymongo import MongoClient
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib
import time

# Kết nối MongoDB (VM4)
client = MongoClient("mongodb://192.168.252.133:27017/?replicaSet=rs0")
db = client["stock_db"]
history_collection = db["stock_history_data"]
realtime_collection = db["stock_realtime_data"]

# Huấn luyện mô hình từ collection lịch sử
print("📚 Đang huấn luyện mô hình từ dữ liệu lịch sử (MongoDB VM4)...")
history_docs = list(history_collection.find({}))

if not history_docs:
    print("❌ Không có dữ liệu lịch sử để huấn luyện.")
    exit()

df = pd.DataFrame(history_docs)
df['movement'] = (df['close'] > df['open']).astype(int)
X = df[["open", "high", "low", "close", "volume"]]
y = df["movement"]

model = RandomForestClassifier()
model.fit(X, y)
print("✅ Mô hình huấn luyện xong. Độ chính xác:", model.score(X, y))
joblib.dump(model, "model.pkl")

# Theo dõi realtime từ collection realtime
print("📡 Đang theo dõi dữ liệu realtime từ MongoDB...")

pipeline = [{'$match': {}}]
change_stream = realtime_collection.watch(pipeline, full_document='updateLookup')

for change in change_stream:
    doc = change.get("fullDocument")
    if not doc:
        continue

    df_stream = pd.DataFrame([doc])
    preds = model.predict(df_stream[["open", "high", "low", "close", "volume"]])
    df_stream["prediction"] = preds
    df_stream["change(%)"] = ((df_stream['close'] - df_stream['open']) / df_stream['open'] * 100).round(2)
    df_stream.to_csv("predicted_output.csv", mode='a', header=False, index=False)
    print("🔥 Dự đoán dòng mới. Ghi vào predicted_output.csv")
