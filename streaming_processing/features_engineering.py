from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import signal
import sys

# Xử lý tín hiệu để dừng script
def signal_handler(sig, frame):
    print("Stopping Spark application...")
    spark.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("StockPriceStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Định nghĩa schema cho dữ liệu JSON
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("symbol", StringType(), True),
    StructField("close_price", DoubleType(), True)
])

# Đọc stream từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.56.102:9092") \
    .option("subscribe", "stock-prices") \
    .option("startingOffsets", "earliest") \
    .load()

# Chuyển đổi dữ liệu JSON thành DataFrame
df_processed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Hiển thị dữ liệu stream (console sink)
query = df_processed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Chờ tín hiệu dừng
try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()
    print("Stopped Spark application.")
