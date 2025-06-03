# Dashboard code will go here
# File: visualization/dashboard.py
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

# Kết nối MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["stock_db"]
collection = db["predictions"]

# Lấy dữ liệu
data = list(collection.find())
df = pd.DataFrame(data)

# Giao diện chính
st.title("📈 Dashboard Dự đoán Giá Cổ Phiếu")
st.subheader("📊 Dữ liệu từ MongoDB")
st.dataframe(df)

# Biểu đồ
st.subheader("📉 Biểu đồ Dự đoán")

if "predicted_price" in df.columns:
    fig, ax = plt.subplots()
    ax.plot(df["input_scaled"], label="Giá đã chuẩn hóa", linestyle="--")
    ax.plot(df["predicted_price"], label="Giá dự đoán", marker="o")
    ax.set_xlabel("Thời điểm")
    ax.set_ylabel("Giá")
    ax.set_title("Dự đoán giá cổ phiếu")
    ax.legend()
    st.pyplot(fig)
else:
    st.warning("❗ Dữ liệu chưa có trường 'predicted_price'")
