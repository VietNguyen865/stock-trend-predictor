# File huấn luyện mô hình dự đoán xu hướng cổ phiếu
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import joblib

def train_model(pandas_df):
    """
    Hàm huấn luyện mô hình RandomForest trên dữ liệu đã cho.

    Tham số:
        pandas_df: DataFrame đã xử lý, có ít nhất 2 cột:
            - 'price_scaled' (feature)
            - 'price' (target)

    Trả về:
        model: mô hình đã huấn luyện
    """

    # Chia dữ liệu train/test
    X = pandas_df[["price_scaled"]]
    y = pandas_df["price"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Khởi tạo mô hình RandomForest
    model = RandomForestRegressor(n_estimators=100, random_state=42)

    # Huấn luyện mô hình
    model.fit(X_train, y_train)

    # Đánh giá mô hình trên tập test
    score = model.score(X_test, y_test)
    print(f"Model R^2 score on test set: {score:.4f}")

    # Lưu mô hình vào file .pkl
    joblib.dump(model, 'stock_model_rf.pkl')
    print("Model saved as 'stock_model_rf.pkl'")

    return model

if __name__ == "__main__":
    # Ví dụ chạy thử với dữ liệu giả lập (bạn thay bằng dữ liệu thực tế từ utils.py)
    # import utils
    # pandas_df = utils.load_clean_data()

    # Giả sử bạn đã có pandas_df ở đây, ví dụ:
    data = {
        "price_scaled": [0.1, 0.2, 0.3, 0.4, 0.5],
        "price": [10, 20, 30, 40, 50],
    }
    pandas_df = pd.DataFrame(data)

    train_model(pandas_df)
