from src.db import engine
from sqlalchemy import text
import pandas as pd


def load_to_csv(query: str, file_name: str) -> str:
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
        df.to_csv(file_name, index=False, encoding="utf-8-sig")
        return file_name

def load_customers():
    query = text("""SELECT external_id, name, phone, email, created_at FROM orders.customers ORDER BY customer_id;""")
    load_to_csv(query, "customers.csv")

def load_drivers():
    query = text("""SELECT external_id, name, phone, vehicle_number, created_at FROM orders.drivers ORDER BY driver_id;""")
    load_to_csv(query, "drivers.csv")

def load_locations():
    query = text("""SELECT address, lat, lon FROM orders.locations ORDER BY location_id;""")
    load_to_csv(query, "locations.csv")

def load_tariffs():
    query = text("""SELECT name, base_price, per_km, created_at FROM orders.tariffs ORDER BY tariff_id;""")
    load_to_csv(query, "tariffs.csv")

def load_statuses():
    query = text("""SELECT name FROM orders.order_statuses ORDER BY status_id;""")
    load_to_csv(query, "statuses.csv")

def load_orders():
    query = text("""SELECT
        o.external_order_id,
        c.external_id AS customer_external_id,  -- Заменили customer_id
        d.external_id AS driver_external_id,    -- Заменили driver_id
        l_start.address AS pickup_address,      -- Заменили pickup_location_id
        l_end.address AS dropoff_address,        -- Заменили dropoff_location_id
        t.name AS tariff_name,                  -- Заменили tariff_id
        o.price,
        o.distance_km,
        o.created_at,
        o.delivered_at,
        o.current_status
    FROM orders.orders o
    JOIN orders.customers c ON o.customer_id = c.customer_id
    LEFT JOIN orders.drivers d ON o.driver_id = d.driver_id  -- LEFT, так как водителя может не быть
    JOIN orders.locations l_start ON o.pickup_location_id = l_start.location_id
    JOIN orders.locations l_end ON o.dropoff_location_id = l_end.location_id
    JOIN orders.tariffs t ON o.tariff_id = t.tariff_id;
    """)
    load_to_csv(query, "orders.csv")

load_customers()
load_drivers()
load_locations()
load_tariffs()
load_statuses()
load_orders()