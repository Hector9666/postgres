# models.py
from sqlalchemy import (
    MetaData,
    Table,
    Identity,
    Column,
    Text,
    Integer,
    TIMESTAMP,
    NUMERIC,
    CheckConstraint,
    ForeignKey,
    Index,
    text
)

from db import engine


metadata = MetaData()
# ===================
# ТАБЛИЦЫ СПРАВОЧНИКИ
# ===================
customers = Table(
    "customers",
    metadata,
    Column("customer_id", Integer, Identity(always=True), primary_key=True),
    Column("external_id", Text, unique=True),
    Column("name", Text, nullable=False),
    Column("phone", Text),
    Column("email", Text),
    Column("created_at", TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"), nullable=False),
)

drivers = Table(
    "drivers",
    metadata,
    Column("driver_id", Integer, Identity(always=True), primary_key=True),
    Column("external_id", Text, unique=True),
    Column("name", Text, nullable=False),
    Column("phone", Text),
    Column("vehicle_number", Text, nullable=False, unique=True),
    Column("created_at", TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"), nullable=False),
)

locations = Table(
    "locations",
    metadata,
    Column("location_id", Integer, Identity(always=True), primary_key=True),
    Column("address", Text, nullable=False, unique=True),
    Column("lat", NUMERIC(precision=9, scale=6), CheckConstraint("lat BETWEEN -90 AND 90"),),
    Column("lon", NUMERIC(precision=9, scale=6), CheckConstraint("lon BETWEEN -180 AND 180"),),
)

tariffs = Table(
    "tariffs",
    metadata,
    Column("tariff_id", Integer, Identity(always=True), primary_key=True),
    Column("name", Text, nullable=False, unique=True),
    Column("base_price", NUMERIC(precision=10, scale=2),
           CheckConstraint("base_price >= 0"), nullable=False, server_defaul="300"),
    Column("per_km", NUMERIC(precision=10, scale=2),
           CheckConstraint("per_km >= 0"), nullable=False, server_defaul="0"),
    Column("created_at", TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP")),
)

order_statuses = Table(
    "order_statuses",
    metadata,
    Column("status_id", Integer, Identity(always=True), primary_key=True),
    Column("name", Text, nullable=False, unique=True),
)

# ==============
# ТАБЛИЦА ФАКТОВ
# ==============
orders = Table(
    "orders",
    metadata,
    Column("order_id", Integer, Identity(always=True), primary_key=True),
    Column("external_order_id", Text, unique=True),
    Column("customer_id", Integer,
           ForeignKey("customers.customer_id", name="fk_customer_id", ondelete="RESTRICT"),
           nullable=False),
    Column("driver_id", Integer,
           ForeignKey("drivers.driver_id", name="fk_driver_id", ondelete="SET NULL")),
    Column("pickup_location_id", Integer,
           ForeignKey("locations.location_id", name="fk_pickup_location_id"), nullable=False),
    Column("dropoff_location_id", Integer,
           ForeignKey("locations.location_id", name="fk_dropoff_location_id"), nullable=False),
    Column("tariff_id", Integer,
           ForeignKey("tariffs.tariff_id", name="fk_tariff_id"), nullable=False),
    Column("price", NUMERIC(precision=10, scale=2), CheckConstraint("price >= 0"), nullable=False),
    Column("distance_km", NUMERIC(precision=7, scale=3), CheckConstraint("distance_km >= 0"), nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("delivered_at", TIMESTAMP(timezone=True)),
    Column("current_status", Integer,
           ForeignKey("order_statuses.status_id", name="fk_status_id"), server_default="1"),
    Index("idx_customer_id", "customer_id"),
    Index("idx_driver_id", "driver_id"),
    Index("idx_created_at", "created_at"),
)

# ===============
# ТАБЛИЦА СОБЫТИЙ
# ===============
order_events = Table(
    "order_events",
    metadata,
    Column("event_id", Integer, Identity(always=True), primary_key=True),
    Column("order_id", Integer,
           ForeignKey("orders.order_id", name="fk_order_id", ondelete="CASCADE"),
           nullable=False),
    Column("status_id", Integer,
           ForeignKey("order_statuses.status_id", name="fk_status_id"), nullable=False),
    Column("event_time", TIMESTAMP(timezone=True), nullable=False),
    Column("note", Text),
)

# ===============
# ТАБЛИЦЫ ДЛЯ ВИТРИН ДАННЫХ
# ===============
dm_drivers_efficiency = Table(
    "dm_drivers_efficiency",
    metadata,
    Column("driver_id", Integer, primary_key=True),
    Column("driver_name", Text, nullable=False),
    Column("vehicle_number", Text),
    Column("total_rides", Integer, nullable=False),
    Column("total_revenue", NUMERIC(precision=12, scale=2), nullable=False),
    Column("avg_order_value", NUMERIC(precision=10, scale=2)),
    Column("total_distance_km", NUMERIC(precision=10, scale=3)),
    Column("revenue_per_km", NUMERIC(precision=10, scale=2)),
)

dm_customer_analytics = Table(
    "dm_customer_analytics",
    metadata,
    Column("customer_id", Integer, primary_key=True),
    Column("customer_name", Text, nullable=False),
    Column("email", Text),
    Column("total_rides", Integer, nullable=False),
    Column("total_spent", NUMERIC(precision=12, scale=2), nullable=False),
    Column("avg_bill", NUMERIC(precision=10, scale=2)),
    Column("total_distance", NUMERIC(precision=10, scale=3)),
    Column("first_ride_at", TIMESTAMP(timezone=True)),
    Column("last_ride_at", TIMESTAMP(timezone=True)),
    Column("customer_lifetime_days", Integer),
)

dm_orders_sla = Table(
    "dm_orders_sla",
    metadata,
    Column("order_id", Integer, primary_key=True),
    Column("external_order_id", Text, nullable=False),
    Column("tariff_name", Text, nullable=False),
    Column("final_status", Text, nullable=False),
    Column("order_created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("order_delivered_at", TIMESTAMP(timezone=True)),
    Column("total_duration_minutes", Integer),
    Column("price", NUMERIC(precision=10, scale=2), nullable=False),
    Column("distance_km", NUMERIC(precision=7, scale=3), nullable=False),
)

def run_schema():
    metadata.create_all(engine)
    print("Schema created")


if __name__ == "__main__":
    run_schema()