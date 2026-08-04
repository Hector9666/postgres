# load_core.py
import os
import shutil

import pandas as pd
from database.db import engine
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from database.models import (
    customers, drivers, locations, tariffs, order_statuses, orders
)

STAGE_DIR = "/tmp/airflow_staging"


def load_from_staging(filename: str) -> pd.DataFrame:
    """Вспомогательная функция для чтения чистых данных."""
    path = os.path.join(STAGE_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Очищенный файл не найден: {path}")
    return pd.read_parquet(path)


# ============================
# Функции загрузки (Задачи для Airflow)
# ============================

def load_customers_task():
    df = load_from_staging("clean_customers.parquet")
    records = df.to_dict("records")
    stmt = pg_insert(customers).values(records).on_conflict_do_nothing(index_elements=["external_id"])
    with engine.begin() as conn:
        conn.execute(stmt)


def load_drivers_task():
    df = load_from_staging("clean_drivers.parquet")
    records = df.to_dict("records")
    stmt = pg_insert(drivers).values(records).on_conflict_do_nothing(index_elements=["external_id"])
    with engine.begin() as conn:
        conn.execute(stmt)


def load_locations_task():
    df = load_from_staging("clean_locations.parquet")
    records = df.to_dict("records")
    stmt = pg_insert(locations).values(records).on_conflict_do_nothing(index_elements=["address"])
    with engine.begin() as conn:
        conn.execute(stmt)


def load_tariffs_task():
    df = load_from_staging("clean_tariffs.parquet")
    records = df.to_dict("records")
    stmt = pg_insert(tariffs).values(records).on_conflict_do_nothing(index_elements=["name"])
    with engine.begin() as conn:
        conn.execute(stmt)


def load_statuses_task():
    df = load_from_staging("clean_statuses.parquet")
    records = df.to_dict("records")
    stmt = pg_insert(order_statuses).values(records).on_conflict_do_nothing(index_elements=["name"])
    with engine.begin() as conn:
        conn.execute(stmt)


def load_orders_task():
    df = load_from_staging("clean_orders.parquet")

    # Логика маппинга (остается БЕЗ изменений)
    with engine.connect() as conn:
        customers_dict = dict(conn.execute(select(customers.c.external_id, customers.c.customer_id)).tuples().all())
        drivers_dict = dict(conn.execute(select(drivers.c.external_id, drivers.c.driver_id)).tuples().all())
        locations_dict = dict(conn.execute(select(locations.c.address, locations.c.location_id)).tuples().all())
        tariffs_dict = dict(conn.execute(select(tariffs.c.name, tariffs.c.tariff_id)).tuples().all())

    df = df.copy()
    df["customer_id"] = df["customer_external_id"].map(customers_dict)
    df["driver_id"] = df["driver_external_id"].map(drivers_dict)
    df["pickup_location_id"] = df["pickup_address"].map(locations_dict)
    df["dropoff_location_id"] = df["dropoff_address"].map(locations_dict)
    df["tariff_id"] = df["tariff_name"].map(tariffs_dict)

    df = df.drop(
        columns=["customer_external_id", "driver_external_id", "pickup_address", "dropoff_address", "tariff_name"])

    if "driver_id" in df.columns:
        df["driver_id"] = df["driver_id"].astype("Int64")

    records = df.to_dict("records")
    stmt = pg_insert(orders).values(records)
    do_update_stmt = stmt.on_conflict_do_update(
        index_elements=["external_order_id"],
        set_={
            "driver_id": stmt.excluded.driver_id,
            "price": stmt.excluded.price,
            "distance_km": stmt.excluded.distance_km,
            "delivered_at": stmt.excluded.delivered_at,
            "current_status": stmt.excluded.current_status
        }
    )

    with engine.begin() as conn:
        conn.execute(do_update_stmt)

    # 🧹 ФИНАЛЬНАЯ САМООЧИСТКА:
    # Как только заказы успешно улетели в базу данных,
    # удаляем всю временную папку со всеми Parquet-файлами
    if os.path.exists(STAGE_DIR):
        shutil.rmtree(STAGE_DIR)
        print(f"♻️ Папка временного стейджинга {STAGE_DIR} автоматически очищена с диска!")


def build_drivers_efficiency_mart_task():
    """Рассчитывает витрину эффективности водителей и сохраняет в физическую таблицу."""

    # Сложный аналитический SQL-запрос для расчета метрик
    sql_query = """
        INSERT INTO dm_drivers_efficiency (
            driver_id, driver_name, vehicle_number, 
            total_rides, total_revenue, avg_order_value, 
            total_distance_km, revenue_per_km
        )
        SELECT 
            d.driver_id,
            d.name AS driver_name,
            d.vehicle_number,
            COUNT(o.order_id) AS total_rides,
            SUM(o.price) AS total_revenue,
            ROUND(AVG(o.price), 2) AS avg_order_value,
            SUM(o.distance_km) AS total_distance_km,
            ROUND(CASE WHEN SUM(o.distance_km) > 0 THEN SUM(o.price) / SUM(o.distance_km) ELSE 0 END, 2) AS revenue_per_km
        FROM orders o
        JOIN drivers d ON o.driver_id = d.driver_id
        GROUP BY d.driver_id, d.name, d.vehicle_number;
    """

    # Выполняем очистку и расчет внутри ОДНОЙ транзакции
    with engine.begin() as conn:
        print("🧹 Очищаем старую витрину...")
        conn.execute(text("TRUNCATE TABLE dm_drivers_efficiency;"))

        print("🚀 Расчитываем и записываем новые данные в витрину...")
        conn.execute(text(sql_query))
        print("🟢 Витрина успешно обновлена!")

def build_customer_analytics_mart_task():
    """Рассчитывает маркетинговую витрину клиентов."""
    sql_query = """
        INSERT INTO dm_customer_analytics (
            customer_id, customer_name, email, total_rides, 
            total_spent, avg_bill, total_distance, 
            first_ride_at, last_ride_at, customer_lifetime_days
        )
        SELECT 
            c.customer_id,
            c.name AS customer_name,
            c.email,
            COUNT(o.order_id) AS total_rides,
            COALESCE(SUM(o.price), 0) AS total_spent,
            ROUND(COALESCE(AVG(o.price), 0), 2) AS avg_bill,
            COALESCE(SUM(o.distance_km), 0) AS total_distance,
            MIN(o.created_at) AS first_ride_at,
            MAX(o.created_at) AS last_ride_at,
            EXTRACT(DAY FROM (MAX(o.created_at) - MIN(o.created_at)))::Integer AS customer_lifetime_days
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.name, c.email;
    """
    with engine.begin() as conn:
        print("🧹 Очищаем витрину клиентов...")
        conn.execute(text("TRUNCATE TABLE dm_customer_analytics;"))
        print("🚀 Запускаем расчет витрины клиентов...")
        conn.execute(text(sql_query))
        print("🟢 Витрина клиентов успешно обновлена!")


def build_orders_sla_mart_task():
    """Рассчитывает операционную витрину SLA по заказам."""
    sql_query = """
        INSERT INTO dm_orders_sla (
            order_id, external_order_id, tariff_name, final_status, 
            order_created_at, order_delivered_at, 
            total_duration_minutes, price, distance_km
        )
        SELECT 
            o.order_id,
            o.external_order_id,
            t.name AS tariff_name,
            s.name AS final_status,
            o.created_at AS order_created_at,
            o.delivered_at AS order_delivered_at,
            ROUND(EXTRACT(EPOCH FROM (o.delivered_at - o.created_at)) / 60)::Integer AS total_duration_minutes,
            o.price,
            o.distance_km
        FROM orders o
        JOIN order_statuses s ON o.current_status = s.status_id
        JOIN tariffs t ON o.tariff_id = t.tariff_id;
    """
    with engine.begin() as conn:
        print("🧹 Очищаем витрину SLA...")
        conn.execute(text("TRUNCATE TABLE dm_orders_sla;"))
        print("🚀 Запускаем расчет витрины SLA...")
        conn.execute(text(sql_query))
        print("🟢 Витрина SLA успешно обновлена!")

if __name__ == "__main__":
    load_customers_task()
    load_drivers_task()
    load_locations_task()
    load_tariffs_task()
    load_statuses_task()
    load_orders_task()
    build_drivers_efficiency_mart_task()
    build_customer_analytics_mart_task()
    build_orders_sla_mart_task()

