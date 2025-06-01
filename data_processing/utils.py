# Các hàm tiện ích phục vụ xử lý dữ liệu
import pandas as pd
from sklearn.preprocessing import StandardScaler

def preprocess_spark_df(spark_df):
    # Chuyển Spark DF sang Pandas DF
    pandas_df = spark_df.toPandas()

    # Xử lý dữ liệu thiếu
    pandas_df.dropna(inplace=True)          # Hoặc: pandas_df.fillna(value=0, inplace=True)

    # Đảm bảo kiểu dữ liệu đúng
    pandas_df["price"] = pandas_df["price"].astype(float)
    pandas_df["timestamp"] = pd.to_datetime(pandas_df["timestamp"])

    # Loại bỏ giá trị bất thường (ví dụ: giá âm)
    pandas_df = pandas_df[pandas_df["price"] > 0]

    # Chuẩn hoá dữ liệu nếu cần
    scaler = StandardScaler()
    pandas_df["price_scaled"] = scaler.fit_transform(pandas_df[["price"]])

    return pandas_df
