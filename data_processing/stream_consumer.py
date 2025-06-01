from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

def main():
    # Tạo SparkSession
    spark = SparkSession.builder \
        .appName("StockStreamProcessor") \
        .getOrCreate()

    # Kết nối Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.xx.xx:9092") \  # Thay bằng IP Kafka của bạn
        .option("subscribe", "stock_topic") \
        .load()

    # Ép kiểu 'value' từ binary -> string
    df_parsed = df.selectExpr("CAST(value AS STRING) as json_str")

    # Định nghĩa schema JSON (ví dụ: dữ liệu có 3 trường)
    schema = StructType([
        StructField("symbol", StringType()),      # Mã chứng khoán
        StructField("price", FloatType()),        # Giá
        StructField("timestamp", StringType())    # Thời gian (hoặc dùng TimestampType nếu có định dạng chuẩn ISO)
    ])

    # Parse JSON
    df_json = df_parsed.withColumn("data", from_json(col("json_str"), schema))

    # Tách cột data thành DataFrame final
    df_final = df_json.select("data.*")

    # Hiển thị dữ liệu trên console (để debug) – có thể bỏ khi chạy thực tế
    query = df_final.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    # Giữ ứng dụng chạy
    query.awaitTermination()

if __name__ == "__main__":
    main()
