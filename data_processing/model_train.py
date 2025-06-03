# File huấn luyện mô hình dự đoán xu hướng cổ phiếu
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import joblib
import sys

def train_model(pandas_df, model_path='stock_model_rf.pkl'):
    """
    Hàm huấn luyện mô hình RandomForest trên dữ liệu đã xử lý.

    Tham số:
        pandas_df: DataFrame đã xử lý, có ít nhất 2 cột:
            - 'price_scaled' (feature)
            - 'price' (target)
        model_path: Tên file lưu mô hình (mặc định: stock_model_rf.pkl)

    Trả về:
        model: mô hình đã huấn luyện
    """

    # Kiểm tra cột bắt buộc
    required_cols = {'price_scaled', 'price'}
    if not required_cols.issubset(pandas_df.columns):
        raise ValueError(f"Dữ liệu phải chứa các cột: {required_cols}")

    # Chia dữ liệu train/test
    X = pandas_df[['price_scaled']]
    y = pandas_df['price']
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Khởi tạo mô hình
    model = RandomForestRegressor(n_estimators=100, random_state=42)

    # Huấn luyện
    model.fit(X_train, y_train)

    # Đánh giá
    score = model.score(X_test, y_test)
    print(f"✅ Model R^2 score on test set: {score:.4f}")

    # In độ quan trọng của đặc trưng
    print("📊 Feature importance:")
    for col, imp in zip(X.columns, model.feature_importances_):
        print(f"  - {col}: {imp:.4f}")

    # Lưu mô hình
    joblib.dump(model, model_path)
    print(f"📁 Model saved as '{model_path}'")

    return model

if __name__ == "__main__":
    # Ví dụ chạy thử
    sample_data = {
        "price_scaled": [0.1, 0.2, 0.3, 0.4, 0.5],
        "price": [10, 20, 30, 40, 50],
    }
    df = pd.DataFrame(sample_data)
    train_model(df)
