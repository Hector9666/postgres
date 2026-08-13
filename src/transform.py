# transform.py
import os
import pandas as pd

STAGE_DIR = "/tmp/airflow_staging"


def load_from_staging(filename: str) -> pd.DataFrame:
    """Читает файл из стейджинга (работает с любым форматом, который передали)."""
    path = os.path.join(STAGE_DIR, filename)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл стейджинга не найден: {path}")

    # Если в аргумент filename передали "stage_orders.parquet",
    # код просто прочитает его напрямую без лишних замен текста
    return pd.read_parquet(path)


def save_transformed_data(df: pd.DataFrame, filename: str) -> str:
    """Сохраняет очищенный DataFrame в формате Parquet."""
    path = os.path.join(STAGE_DIR, filename)

    # Сохраняем напрямую по указанному пути
    df.to_parquet(path, index=False)
    print(f"🔹 Очищенные данные сохранены в Parquet: {path}")
    return path


# ============================
# Заказы
# ============================
def process_orders_task():
    """Задача для Airflow: Читает сырые заказы, чистит и сохраняет."""
    # Читаем то, что подготовил extract.py
    df = load_from_staging("stage_orders.parquet")

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

    # Сохраняем результат для load.py
    return save_transformed_data(df, "clean_orders.parquet")


# ============================
# Покупатели
# ============================
def process_customers_task():
    df = load_from_staging("stage_customers.parquet")

    df["external_id"] = df["external_id"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df["phone"] = df["phone"].astype(str).str.strip()
    df["email"] = df["email"].astype(str).str.strip()
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    return save_transformed_data(df, "clean_customers.parquet")


# ============================
# Водители
# ============================
def process_drivers_task():
    df = load_from_staging("stage_drivers.parquet")

    df["external_id"] = df["external_id"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df["phone"] = df["phone"].astype(str).str.strip()
    df["vehicle_number"] = (
        df["vehicle_number"].astype(str).str.upper().str.strip()
    )
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    return save_transformed_data(df, "clean_drivers.parquet")

# ============================
# Адреса
# ============================

def process_locations_task():
    df = load_from_staging("stage_locations.parquet")

    df["address"] = df["address"].astype(str).str.strip()
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    return save_transformed_data(df, "clean_locations.parquet")


# ============================
# Тарифы
# ============================

def process_tariffs_task():
    df = load_from_staging("stage_tariffs.parquet")

    df["name"] = df["name"].astype(str).str.strip()
    df["base_price"] = pd.to_numeric(df["base_price"], errors="coerce")
    df["per_km"] = pd.to_numeric(df["per_km"], errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    return save_transformed_data(df, "clean_tariffs.parquet")


# ============================
# Статусы
# ============================

def process_statuses_task():
    df = load_from_staging("stage_statuses.parquet")

    df["name"] = df["name"].astype(str).str.strip().str.lower()

    return save_transformed_data(df, "clean_statuses.parquet")


# ============================
# Проверка
# ============================

if __name__ == "__main__":
    process_customers_task()
    process_drivers_task()
    process_locations_task()
    process_tariffs_task()
    process_statuses_task()
    process_orders_task()
