# extract_daily_orders.py
import os
import pandas as pd

# Внутри контейнера Airflow папка с вашими сырыми CSV-файлами должна быть доступна.
# Например, можно положить их в папку dags/data/raw/
# RAW_DATA_DIR = "/opt/airflow/data/raw/daily_orders"
# BRONZE_DIR = "opt/airflow/data/bronze/orders"
RAW_DATA_DIR = "data/raw/daily_orders"
BRONZE_DIR = "data/bronze/orders"


def load_csv(filename: str) -> pd.DataFrame:
    """Читает CSV из RAW_DATA_DIR и возвращает DataFrame."""
    path = os.path.join(RAW_DATA_DIR, filename)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл не найден: {path}")

    df = pd.read_csv(path)
    return df


def save_to_bronze(df: pd.DataFrame, date_str: str, bronze_filename: str) -> str:
    """Сохраняет DataFrame на диск в формате Parquet."""
    bronze_filepath = os.path.join(BRONZE_DIR, date_str, bronze_filename)
    os.makedirs(os.path.dirname(bronze_filepath), exist_ok=True)

    # Сохраняем в бинарный формат Parquet
    df.to_parquet(bronze_filepath, index=False)
    print(f"🔹 Файл успешно сохранен в слой Bronze: {bronze_filepath}")
    return bronze_filepath


# ============================
# Заказы
# ============================
def extract_orders(date_str) -> str:
    """Загружает сырые данные заказов и сохраняет в стейджинг."""
    df = load_csv(f"orders_{date_str}.csv")

    if df.empty:
        raise ValueError(f"orders_{date_str}.csv пустой — нечего обрабатывать")

    required_columns = [
        "external_order_id",
        "customer_external_id",
        "driver_external_id",
        "pickup_address",
        "dropoff_address",
        "tariff_name",
        "price",
        "distance_km",
        "created_at",
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"В orders_{date_str}.csv отсутствуют колонки: {missing}")

    # Вместо return df мы сохраняем его на диск
    return save_to_bronze(df, date_str, f"bronze_orders_{date_str}.parquet")

# ============================
# Проверка
# ============================

if __name__ == "__main__":
    extract_orders("2026_08_05")

