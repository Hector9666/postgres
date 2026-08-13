# transform_daily_orders.py
import os
import pandas as pd

BRONZE_DIR = "data/bronze/orders"
SILVER_DIR = "data/silver/orders"


def load_from_bronze(date_str: str, filename: str) -> pd.DataFrame:
    """Читает файл из слоя Bronze."""
    path = os.path.join(BRONZE_DIR, date_str, filename)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл стейджинга не найден: {path}")

    return pd.read_parquet(path)


def save_to_silver(df: pd.DataFrame, date_str, filename: str) -> str:
    """Сохраняет очищенный DataFrame в формате Parquet."""
    silver_filepath = os.path.join(SILVER_DIR, date_str, filename)
    os.makedirs(os.path.dirname(silver_filepath), exist_ok=True)

    # Сохраняем напрямую по указанному пути
    df.to_parquet(silver_filepath, index=False)
    print(f'Очищенные данные сохранены в Parquet: {silver_filepath}')
    return silver_filepath


# ============================
# Заказы
# ============================
def transform_daily_orders(date_str: str):
    """Задача для Airflow: Читает сырые заказы, чистит и сохраняет."""
    # Читаем то, что подготовил extract_daily_orders.py
    df = load_from_bronze(date_str,f"bronze_orders_{date_str}.parquet")

    # Бизнес-логика трансформации
    df = df.drop_duplicates(subset=["external_order_id"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["distance_km"] = pd.to_numeric(df["distance_km"], errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # Стрипаем строки
    for col_name in [
        "external_order_id",
        "customer_external_id",
        "driver_external_id",
        "pickup_address",
        "dropoff_address",
        "tariff_name",
    ]:
        df[col_name] = df[col_name].astype(str).str.strip()

    df = df.dropna(subset=["external_order_id", "created_at", "price"])

    # Сохраняем результат для load_daily_orders.py
    return save_to_silver(df, date_str, f"silver_orders_{date_str}.parquet")


# ============================
# Проверка
# ============================

if __name__ == "__main__":
    transform_daily_orders("2026_08_05")
