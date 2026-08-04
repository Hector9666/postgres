from models import metadata
from db import engine

def init_db():
    metadata.create_all(engine)
    print("Schema created")


if __name__ == "__main__":
    init_db()