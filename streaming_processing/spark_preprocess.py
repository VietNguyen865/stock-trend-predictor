from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("Stock Preprocessing").getOrCreate()

# Đọc file CSV
df = spark.read.csv("data_collection/sample_data.csv", header=True, inferSchema>

# Loại bỏ dòng thiếu dữ liệu
df_clean = df.dropna()

# Tạo cột 'movement': tăng = 1, giảm = 0
df_feat = df_clean.withColumn("movement", when(col("close") > col("open"), 1).o>

# Xuất dữ liệu ra file cho VM5 sử dụng
df_feat.toPandas().to_csv("ml_model/processed_data.csv", index=False)

spark.stop()
