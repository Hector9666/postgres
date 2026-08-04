from database.models import metadata
from database.db import engine

def init_db():
    # ВНИМАНИЕ: Это удалит ВСЕ таблицы и ВСЕ данные в них!
    metadata.drop_all(engine)
    print("База данных успешно удалена")

    # Создает чистые таблицы с новой структурой
    metadata.create_all(engine)
    print("База данных успешно пересоздана с актуальной структурой")


if __name__ == "__main__":
    init_db()