# extract.py
import os
import pandas as pd

# Внутри контейнера Airflow папка с вашими сырыми CSV-файлами должна быть доступна.
# Например, можно положить их в папку dags/data/raw/
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "raw")
# Папка для промежуточных (staging) файлов перед трансформацией
STAGE_DIR = "/tmp/airflow_staging"

# Создаем папку для стейджинга, если её нет
os.makedirs(STAGE_DIR, exist_ok=True)


def load_csv(filename: str) -> pd.DataFrame:
    """Читает CSV из RAW_DATA_DIR и возвращает DataFrame."""
    path = os.path.join(RAW_DATA_DIR, filename)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл не найден: {path}")

    df = pd.read_csv(path)
    return df


def save_to_staging(df: pd.DataFrame, stage_filename: str) -> str:
    """Сохраняет DataFrame на диск в формате Parquet."""
    # Меняем расширение на .parquet
    parquet_filename = stage_filename.replace(".csv", ".parquet")
    output_path = os.path.join(STAGE_DIR, parquet_filename)

    # Сохраняем в бинарный формат Parquet
    df.to_parquet(output_path, index=False)
    print(f"🔹 Файл успешно сохранен в Parquet-стейджинг: {output_path}")
    return output_path


# ============================
# Заказы
# ============================
def extract_orders() -> str:
    """Загружает сырые данные заказов и сохраняет в стейджинг."""
    df = load_csv("orders.csv")

    if df.empty:
        raise ValueError("orders.csv пустой — нечего обрабатывать")

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
        raise ValueError(f"В orders.csv отсутствуют колонки: {missing}")

    # Вместо return df мы сохраняем его на диск
    return save_to_staging(df, "stage_orders.parquet")


# ============================
# Покупатели
# ============================
def extract_customers() -> str:
    df = load_csv("customers.csv")

    required = ["external_id", "name", "phone", "email", "created_at"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"В customers.csv отсутствуют колонки: {missing}")

    return save_to_staging(df, "stage_customers.parquet")


# ============================
# Водители
# ============================
def extract_drivers() -> str:
    df = load_csv("drivers.csv")

    required = ["external_id", "name", "phone", "vehicle_number", "created_at"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"В drivers.csv отсутствуют колонки: {missing}")

    return save_to_staging(df, "stage_drivers.parquet")

# ============================
# Адреса
# ============================

def extract_locations() -> pd.DataFrame:
    df = load_csv("locations.csv")

    required = ["address", "lat", "lon"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"В locations.csv отсутствуют колонки: {missing}")

    save_to_staging(df, "stage_locations.parquet")


# ============================
# Тарифы
# ============================

def extract_tariffs() -> pd.DataFrame:
    df = load_csv("tariffs.csv")

    required = ["name", "base_price", "per_km", "created_at"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"В tariffs.csv отсутствуют колонки: {missing}")

    save_to_staging(df, "stage_tariffs.parquet")


# ============================
# Статусы
# ============================

def extract_statuses() -> pd.DataFrame:
    df = load_csv("statuses.csv")

    required = ["name"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"В statuses.csv отсутствуют колонки: {missing}")

    save_to_staging(df, "stage_statuses.parquet")


# ============================
# Проверка
# ============================

if __name__ == "__main__":
    extract_orders()
    extract_customers()
    extract_drivers()
    extract_locations()
    extract_tariffs()
    extract_statuses()
