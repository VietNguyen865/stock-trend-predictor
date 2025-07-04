from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import signal
import sys

# Xử lý tín hiệu để dừng script
def signal_handler(sig, frame):
    print("Stopping Spark application...")
    if 'query' in globals():
        query.stop()
    spark.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Khởi tạo Spark Session
try:
    spark = SparkSession.builder \
        .appName("StockPriceStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "4") \  # Tăng partitions cho hiệu suất
        .getOrCreate()
    print("Spark Session initialized successfully.")
except Exception as e:
    print(f"Error initializing Spark Session: {e}")
    sys.exit(1)

# Định nghĩa schema cho dữ liệu JSON
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("symbol", StringType(), True),
    StructField("close_price", DoubleType(), True)
])

# Đọc stream từ Kafka
try:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.56.102:9092") \
        .option("subscribe", "stock-prices") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \  # Ngăn crash nếu mất dữ liệu
        .load()
    print("Kafka stream connected successfully.")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    spark.stop()
    sys.exit(1)

# Chuyển đổi dữ liệu JSON thành DataFrame
df_processed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Hiển thị dữ liệu stream (console sink) với checkpoint
query = df_processed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \  # Thư mục checkpoint tạm
    .start()

# Chờ tín hiệu dừng
try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()
    print("Stopped Spark application.")
except Exception as e:
    print(f"Error during stream processing: {e}")
    query.stop()
    spark.stop()
    sys.exit(1)
