from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, stddev, lag
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window
import signal
import sys

# ... (Xử lý tín hiệu như trước)

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("StockPriceFeatureEngineering") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Định nghĩa schema
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("symbol", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

# Đọc stream từ cả hai topic
df_historical = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.56.102:9092") \
    .option("subscribe", "stock-prices-historical") \
    .option("startingOffsets", "earliest") \
    .load()

df_realtime = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.56.102:9092") \
    .option("subscribe", "stock-prices") \
    .option("startingOffsets", "earliest") \
    .load()

# Kết hợp dữ liệu
df_combined = df_historical.union(df_realtime)

# Chuyển đổi JSON
df_processed = df_combined.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Làm sạch dữ liệu
df_cleaned = df_processed.filter(col("close") > 0)

# Tạo cửa sổ thời gian
window_spec = Window \
    .partitionBy("symbol") \
    .orderBy("timestamp") \
    .rowsBetween(-4, 0)  # Cửa sổ 5 phút

# Tạo đặc trưng
df_features = df_cleaned.withColumn("avg_price", avg("close").over(window_spec)) \
                       .withColumn("std_price", stddev("close").over(window_spec)) \
                       .withColumn("price_change", col("close") - lag("close", 1).over(window_spec))

# Hiển thị hoặc lưu (console sink)
query = df_features.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/home/user/spark_checkpoint") \
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
