# Placeholder for Kafka producer
import yfinance as yf
import json
import time
from kafka import KafkaProducer

# Lấy dữ liệu giá cổ phiếu
def fetch_stock_data(symbol):
    stock = yf.Ticker(symbol)
    data = stock.history(period="1d", interval="1m")
    return data

# Cấu hình Kafka producer
producer = KafkaProducer(
    bootstrap_servers='192.168.56.102:9092',  # IP của VM2 (sẽ cập nhật sau khi setup VM2)
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Thu thập và gửi dữ liệu liên tục
symbol = "AAPL"
topic = "stock-prices"

while True:
    try:
        stock_data = fetch_stock_data(symbol)
        stock_data_json = [
            {"timestamp": str(index), "symbol": symbol, "close_price": row["Close"]}
            for index, row in stock_data.iterrows()
        ]

        for data in stock_data_json:
            producer.send(topic, data)
            print(f"Sent: {data}")
        producer.flush()

        time.sleep(60)  # Chờ 1 phút
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(60)

producer.close()
