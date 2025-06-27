
import yfinance as yf
import json
import time
import signal
import sys
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Xử lý tín hiệu để đóng producer khi dừng script
def signal_handler(sig, frame):
    print("Closing producer...")
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Lấy dữ liệu giá cổ phiếu
def fetch_stock_data(symbol):
    try:
        stock = yf.Ticker(symbol)
        data = stock.history(period="1d", interval="1m")
        if data.empty:
            raise ValueError(f"No data returned for symbol {symbol}")
        return data
    except Exception as e:
        raise Exception(f"Failed to fetch stock data: {e}")

# Cấu hình Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers='192.168.56.102:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except NoBrokersAvailable:
    print("Error: Cannot connect to Kafka server at 192.168.56.102:9092")
    sys.exit(1)

# Thu thập và gửi dữ liệu liên tục
symbol = "AAPL"
topic = "stock-prices"
SLEEP_INTERVAL = 60  # Giây

while True:
    try:
        stock_data = fetch_stock_data(symbol)
        stock_data_json = [
            {"timestamp": index.strftime('%Y-%m-%d %H:%M:%S'), "symbol": symbol, "close_price": row["Close"]}
            for index, row in stock_data.iterrows()
        ]

        for data in stock_data_json:
            producer.send(topic, data).get(timeout=10)  # Đợi xác nhận gửi
            print(f"Sent: {data}")
        producer.flush()

        time.sleep(SLEEP_INTERVAL)
    except Exception as e:
        print(f"Error fetching or sending data: {e}")
        time.sleep(SLEEP_INTERVAL)
        continue
