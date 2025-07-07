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

# Huấn luyện mô hình từ dữ liệu lịch sử (theo từng symbol)
print("📚 Đang huấn luyện mô hình từ dữ liệu lịch sử (MongoDB VM4)...")
history_docs = list(history_collection.find({}))

if not history_docs:
    print("❌ Không có dữ liệu lịch sử để huấn luyện.")
    exit()

df_all = pd.DataFrame(history_docs)

if 'symbol' not in df_all.columns:
    print("❌ Dữ liệu thiếu trường 'symbol'.")
    exit()

models = {}  # Lưu model theo từng symbol

for symbol in df_all['symbol'].unique():
    df = df_all[df_all['symbol'] == symbol].copy()
    df['movement'] = (df['close'] > df['open']).astype(int)
    X = df[["open", "high", "low", "close", "volume"]]
    y = df["movement"]

    model = RandomForestClassifier()
    model.fit(X, y)
    models[symbol] = model
    print(f"✅ Huấn luyện xong mô hình cho mã {symbol}, độ chính xác: {model.score(X, y):.2f}")
    joblib.dump(model, f"model_{symbol}.pkl")

# Theo dõi realtime
print("📡 Đang theo dõi dữ liệu realtime từ MongoDB...")

pipeline = [{'$match': {}}]
change_stream = realtime_collection.watch(pipeline, full_document='updateLookup')

for change in change_stream:
    doc = change.get("fullDocument")
    if not doc:
        continue

    symbol = doc.get("symbol", "UNKNOWN")
    if symbol not in models:
        print(f"⚠️ Bỏ qua mã cổ phiếu chưa có mô hình huấn luyện: {symbol}")
        continue

    df_stream = pd.DataFrame([doc])
    model = models[symbol]
    preds = model.predict(df_stream[["open", "high", "low", "close", "volume"]])
    df_stream["prediction"] = preds
    df_stream["change(%)"] = ((df_stream['close'] - df_stream['open']) / df_stream['open'] * 100).round(2)

    # Ghi kết quả vào collection riêng theo symbol
    collection_name = f"predicted_output_{symbol}"
    db[collection_name].insert_many(df_stream.to_dict('records'))

    print(f"🔥 Dự đoán xong mã {symbol}, ghi vào collection {collection_name}")
