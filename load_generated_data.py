from sqlalchemy import text
from db import engine

def load_generated_data():
    # Читаем generated_data.sql
    with open("generated_data.sql", 'r', encoding="UTF-8") as f:
        query = f.read()

    # Выполняем скрипт
    with engine.begin() as conn:
        conn.execute(text(query))

    print("Data loaded")

if __name__ == "__main__":
    load_generated_data()