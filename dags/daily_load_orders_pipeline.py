#daily_load_orders_pipline.py
from airflow.sdk import dag, task
import pendulum

# Импортируем задачи из адаптированных модулей
from src import load_daily_orders, transform_daily_orders, extract_daily_orders


default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2026, 8, 1),
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}
@dag(
        dag_id="etl_daily_orders_pipeline",
        default_args=default_args,
        schedule="@daily",
        catchup=False,
        max_active_tasks=3,   # Одновременно в системе будет работать максимум 3 квадратика
        max_active_runs=1,    # Если запустится ручной и автоматический ран, они будут стоять в очереди и выполняться СТРОГО по очереди, а не вместе
        tags=["taxi", "postges", "pandas"],
)
def taxi_etl_daily_orders_pipeline():
    # --- 1. СЛОЙ BRONZE (EXTRACT) ---
    @task(task_id="extract_daily_orders")
    def ext_orders(data_interval_start=None):
        extract_daily_orders.extract_orders(date_str=data_interval_start.strftime('%Y_%m_%d'))

    # --- 2. СЛОЙ SILVER (TRANSFORM) ---
    @task(task_id="transform_daily_orders")
    def trf_orders(data_interval_start=None):
        transform_daily_orders.transform_daily_orders(date_str=data_interval_start.strftime('%Y_%m_%d'))

    # --- 3. СЛОЙ GOLD (LOAD CORE) ---
    @task(task_id="load_daily_orders")
    def ld_orders(data_interval_start=None):
        load_daily_orders.load_daily_orders(date_str=data_interval_start.strftime('%Y_%m_%d'))

    # --- 4. СЛОЙ MARTS (ANALYTICS) ---
    @task(task_id="drivers_mart")
    def ld_drivers_mart(): load_daily_orders.build_drivers_efficiency_mart_task()
    @task(task_id="customers_mart")
    def ld_customers_mart(): load_daily_orders.build_customer_analytics_mart_task()
    @task(task_id="sla_mart")
    def ld_sla_mart(): load_daily_orders.build_orders_sla_mart_task()

    # ---------------------------------------------------------
    # НАСТРОЙКА ГРАФА ЗАВИСИМОСТЕЙ ЧЕРЕЗ ПРЯМОЙ ВЫЗОВ
    # ---------------------------------------------------------

    # Инициализируем таски. Airflow подставит контекст во все три функции
    task_extract = ext_orders()
    task_transform = trf_orders()
    task_load = ld_orders()

    # Строим последовательную цепочку: Extract -> Transform -> Load
    task_extract >> task_transform >> task_load

    # Витрины запускаются параллельно СТРОГО после успешной загрузки в Gold (task_load)
    task_load >> [ld_drivers_mart(), ld_customers_mart(), ld_sla_mart()]


# Компиляция конвейера
taxi_etl_daily_orders_pipeline()
