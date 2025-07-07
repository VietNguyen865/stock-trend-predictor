import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
import sys
import os

# Cấu hình Kafka Producer
def setup_kafka_producer(bootstrap_servers='192.168.56.102:9092'):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except NoBrokersAvailable:
        print("Error: Cannot connect to Kafka server. Exiting.")
        sys.exit(1)

# Lấy dữ liệu thời gian thực
def fetch_realtime_data(symbol):
    try:
        stock = yf.Ticker(symbol)
        data = stock.history(period="1d", interval="1m")
        if data.empty:
            print(f"Warning: No realtime data for {symbol}")
            return None
        data = data.reset_index()
        data["symbol"] = symbol
        return data
    except Exception as e:
        print(f"Error fetching realtime data for {symbol}: {e}")
        return None

# Lấy dữ liệu lịch sử 7 ngày
def fetch_historical_data(symbol):
    try:
        stock = yf.Ticker(symbol)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        data = stock.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), interval="1m")
        if data.empty:
            print(f"Warning: No historical data for {symbol}")
            return None
        data = data.reset_index()
        data["symbol"] = symbol
        return data
    except Exception as e:
        print(f"Error fetching historical data for {symbol}: {e}")
        return None

# Hàm chính
def main():
    # Danh sách mã cổ phiếu
    STOCK_SYMBOLS = ["AAPL", "MSFT", "GOOGL"]

    # Thiết lập Kafka Producer
    producer = setup_kafka_producer()

    # Lấy và xử lý dữ liệu lịch sử 7 ngày
    historical_data = []
    for symbol in STOCK_SYMBOLS:
        data = fetch_historical_data(symbol)
        if data is not None:
            historical_data.append(data)

    if historical_data:
        df_historical = pd.concat(historical_data, ignore_index=True)
        historical_file = f"data_collection/historical_stock_data_7d_{datetime.now().strftime('%Y%m%d')}.csv"
        df_historical.to_csv(historical_file, index=False)
        print(f"Đã lưu dữ liệu lịch sử 7 ngày vào {historical_file}")

        # Gửi qua Kafka topic stock-prices-historical
        topic_historical = "stock-prices-historical"
        for _, row in df_historical.iterrows():
            data = {
                "timestamp": row["Date"].strftime('%Y-%m-%d %H:%M:%S'),
                "symbol": row["symbol"],
                "open": row["Open"],
                "high": row["High"],
                "low": row["Low"],
                "close": row["Close"],
                "volume": row["Volume"]
            }
            producer.send(topic_historical, data)

    # Lấy và xử lý dữ liệu thời gian thực (vòng lặp vô hạn)
    while True:
        realtime_data = []
        for symbol in STOCK_SYMBOLS:
            data = fetch_realtime_data(symbol)
            if data is not None:
                realtime_data.append(data)

        if realtime_data:
            df_realtime = pd.concat(realtime_data, ignore_index=True)
            realtime_file = f"data_collection/realtime_stock_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df_realtime.to_csv(realtime_file, index=False)
            print(f"Đã lưu dữ liệu thời gian thực vào {realtime_file}")

            # Gửi qua Kafka topic stock-prices
            topic_realtime = "stock-prices"
            for _, row in df_realtime.iterrows():
                data = {
                    "timestamp": row["Date"].strftime('%Y-%m-%d %H:%M:%S'),
                    "symbol": row["symbol"],
                    "open": row["Open"],
                    "high": row["High"],
                    "low": row["Low"],
                    "close": row["Close"],
                    "volume": row["Volume"]
                }
                producer.send(topic_realtime, data)

        producer.flush()
        time.sleep(60)  # Chờ 1 phút trước khi lấy dữ liệu mới

    producer.close()

if __name__ == "__main__":
    main()
