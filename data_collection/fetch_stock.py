# Cài đặt thư viện cần thiết
!pip install yfinance pandas kafka-python

import yfinance as yf
import json
import time
import signal
import sys
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Xử lý tín hiệu để dừng script
def signal_handler(sig, frame):
    print("Closing producer...")
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Lấy dữ liệu giá cổ phiếu từ yfinance
def fetch_stock_data(symbol):
    try:
        stock = yf.Ticker(symbol)
        data = stock.history(period="1d", interval="1m")
        if data.empty:
            raise ValueError(f"Không có dữ liệu trả về cho mã {symbol}")
        return data
    except Exception as e:
        raise Exception(f"Lỗi khi lấy dữ liệu cổ phiếu {symbol}: {e}")

# Cấu hình Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers='192.168.56.102:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except NoBrokersAvailable:
    print("Error: Cannot connect to Kafka server at 192.168.56.102:9092")
    sys.exit(1)

# Danh sách mã cổ phiếu
STOCK_SYMBOLS = ["AAPL", "MSFT", "GOOGL"]
last_timestamps = {symbol: None for symbol in STOCK_SYMBOLS}  # Theo dõi thời gian cuối cùng

# Thu thập và gửi dữ liệu liên tục
topic = "stock-prices"
SLEEP_INTERVAL = 60  # Giây

while True:
    try:
        for symbol in STOCK_SYMBOLS:
            print(f"Lấy dữ liệu cho mã {symbol}...")
            stock_data = fetch_stock_data(symbol)
            stock_data_json = [
                {"timestamp": index.strftime('%Y-%m-%d %H:%M:%S'), "symbol": symbol, "close_price": row["Close"]}
                for index, row in stock_data.iterrows()
                if last_timestamps[symbol] is None or index > last_timestamps[symbol]
            ]

            if stock_data_json:
                for data in stock_data_json:
                    producer.send(topic, data).get(timeout=10)  # Đợi xác nhận gửi
                    print(f"Sent: {data}")
                last_timestamps[symbol] = stock_data.index[-1]  # Cập nhật thời gian cuối cùng
            else:
                print(f"Không có dữ liệu mới cho {symbol}")
            producer.flush()

        time.sleep(SLEEP_INTERVAL)
    except Exception as e:
        print(f"Error fetching or sending data: {e}")
        time.sleep(SLEEP_INTERVAL)
        continue
