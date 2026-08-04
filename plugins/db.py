import os
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

# Проверяем, запущен ли код внутри контейнера Airflow
IS_INSIDE_DOCKER = os.getenv("AIRFLOW_CONFIG") is not None

if IS_INSIDE_DOCKER:
    # Настройки для сети Docker (внутренние)
    DB_HOST = "postgres"
    DB_PORT = 5432
else:
    # Настройки для локального ПК (внешние)
    DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT = int(os.getenv("POSTGRES_PORT", 5434))

DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PW") or "pgpass"
DB_NAME = os.getenv("POSTGRES_DB", "postgres")

# Создаем движок
from sqlalchemy import create_engine, text, URL

DB_URL = URL.create(
     "postgresql+psycopg2",
     username=DB_USER,
     password=DB_PASS,
     host=DB_HOST,
     database=DB_NAME,
     port=DB_PORT,
)

engine = create_engine(DB_URL, echo=True)


def check_connection():
 try:
  with engine.connect() as conn:
   result = conn.execute(text("SELECT 1"))
   print("DB connection OK, result:", list(result))
 except Exception as e:
  print("DB connection FAILED:", e)


if __name__ == "__main__":
    check_connection()