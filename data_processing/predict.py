# File dự đoán xu hướng dựa trên mô hình đã huấn luyện
# File: predict.py
import joblib
import pandas as pd
from pymongo import MongoClient

def predict_and_store(pandas_df, model_path="stock_model_rf.pkl"):
    # Tải mô hình
    model = joblib.load(model_path)

    # Chọn feature để dự đoán
    X = pandas_df[["price_scaled"]]

    # Dự đoán
    y_pred = model.predict(X)

    # Kết nối MongoDB
    client = MongoClient("mongodb://localhost:27017")
    db = client["stock_db"]
    collection = db["predictions"]

    # Tạo danh sách document
    prediction_docs = []
    for i in range(len(X)):
        doc = {
            "input_scaled": float(X.iloc[i]["price_scaled"]),
            "predicted_price": float(y_pred[i])
        }
        prediction_docs.append(doc)

    # Lưu vào MongoDB
    collection.insert_many(prediction_docs)
    print(f"✅ Đã lưu {len(prediction_docs)} bản ghi dự đoán vào MongoDB.")

if __name__ == "__main__":
    # Ví dụ dữ liệu
    data = {
        "price_scaled": [0.1, 0.2, 0.3, 0.4, 0.5]
    }
    df = pd.DataFrame(data)
    predict_and_store(df)
