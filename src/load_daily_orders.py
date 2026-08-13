# load_daily_orders.py
import os
import pandas as pd
from database.db import engine
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from database.models import (
    customers, drivers, locations, tariffs, orders
)

SILVER_DIR = "data/silver/orders"

def load_from_silver(date_str: str, filename: str) -> pd.DataFrame:
    """Вспомогательная функция для чтения чистых данных."""
    silver_path = os.path.join(SILVER_DIR, date_str, filename)

    if not os.path.exists(silver_path):
        raise FileNotFoundError(f"Очищенный файл не найден: {silver_path}")
    return pd.read_parquet(silver_path)


# ============================
# Функции загрузки (Задачи для Airflow)
# ============================

def load_daily_orders(date_str: str):
    df = load_from_silver(date_str,f"silver_orders_{date_str}.parquet")

    # Логика маппинга
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
    load_daily_orders("2026_08_11")
    build_drivers_efficiency_mart_task()
    build_customer_analytics_mart_task()
    build_orders_sla_mart_task()

