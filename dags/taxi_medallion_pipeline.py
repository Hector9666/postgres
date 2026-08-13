#taxi_medalion_pipline.py
from airflow.sdk import dag, task
import pendulum

# Гарантируем, что Airflow видит наши модули в папке dags
# sys.path.append(os.path.dirname(__file__))

# Импортируем задачи из адаптированных модулей
from src import load_core as ld, transform as trf, extract as ext
from database.init_database import init_db

default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2026, 8, 1),
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}
@dag(
        dag_id="taxi_etl_medallion",
        default_args=default_args,
        schedule=None,
        catchup=False,
        max_active_tasks=3,   # Одновременно в системе будет работать максимум 3 квадратика
        max_active_runs=1,    # Если запустится ручной и автоматический ран, они будут стоять в очереди и выполняться СТРОГО по очереди, а не вместе
        tags=["taxi", "postges", "pandas"],
)
def taxi_etl_medallion_pipeline():
    # --- 0. Инициализация базы данных
    @task(task_id="init_database")
    def start_init_db(): init_db()
    # --- 1. СЛОЙ BRONZE (EXTRACT) ---
    @task(task_id="extract_customers")
    def ext_customers(): ext.extract_customers()
    @task(task_id="extract_drivers")
    def ext_drivers(): ext.extract_drivers()
    @task(task_id="extract_locations")
    def ext_locations(): ext.extract_locations()
    @task(task_id="extract_tariffs")
    def ext_tariffs(): ext.extract_tariffs()
    @task(task_id="extract_statuses")
    def ext_statuses(): ext.extract_statuses()
    @task(task_id="extract_orders")
    def ext_orders(): ext.extract_orders()

    # --- 2. СЛОЙ SILVER (TRANSFORM) ---
    @task(task_id="transform_customers")
    def trf_customers(): trf.process_customers_task()
    @task(task_id="transform_drivers")
    def trf_drivers(): trf.process_drivers_task()
    @task(task_id="transform_locations")
    def trf_locations(): trf.process_locations_task()
    @task(task_id="transform_tariffs")
    def trf_tariffs(): trf.process_tariffs_task()
    @task(task_id="transform_statuses")
    def trf_statuses(): trf.process_statuses_task()
    @task(task_id="transform_orders")
    def trf_orders(): trf.process_orders_task()

    # --- 3. СЛОЙ GOLD (LOAD CORE) ---
    @task(task_id="load_customers")
    def ld_customers(): ld.load_customers_task()
    @task(task_id="load_drivers")
    def ld_drivers(): ld.load_drivers_task()
    @task(task_id="load_locations")
    def ld_locations(): ld.load_locations_task()
    @task(task_id="load_tariffs")
    def ld_tariffs(): ld.load_tariffs_task()
    @task(task_id="load_statuses")
    def ld_statuses(): ld.load_statuses_task()
    @task(task_id="load_orders")
    def ld_orders(): ld.load_orders_task()

    # --- 4. СЛОЙ MARTS (ANALYTICS) ---
    @task(task_id="drivers_mart")
    def ld_drivers_mart(): ld.build_drivers_efficiency_mart_task()
    @task(task_id="customers_mart")
    def ld_customers_mart(): ld.build_customer_analytics_mart_task()
    @task(task_id="sla_mart")
    def ld_sla_mart(): ld.build_orders_sla_mart_task()

    # ---------------------------------------------------------
    # НАСТРОЙКА ГРАФА ЗАВИСИМОСТЕЙ ЧЕРЕЗ ПРЯМОЙ ВЫЗОВ
    # ---------------------------------------------------------
    # Вызываем функции и связываем их в цепочки
    start_init = start_init_db()
    customer_gold = start_init >> ext_customers() >> trf_customers() >> ld_customers()
    driver_gold = start_init >> ext_drivers() >> trf_drivers() >> ld_drivers()
    location_gold = start_init >> ext_locations() >> trf_locations() >> ld_locations()
    tariff_gold = start_init >> ext_tariffs() >> trf_tariffs() >> ld_tariffs()
    status_gold = start_init >> ext_statuses() >> trf_statuses() >> ld_statuses()

    # Заказы проходят Silver
    order_silver = ext_orders() >> trf_orders()

    # Главная связка: Направляем список готовых Gold-справочников и Silver-заказов в Gold-заказы
    [
        customer_gold,
        driver_gold,
        location_gold,
        tariff_gold,
        status_gold,
        order_silver
    ] >> ld_orders() >> [ld_drivers_mart(), ld_customers_mart(), ld_sla_mart()]

taxi_etl_medallion_pipeline()

# with DAG(
#         dag_id="taxi_etl_medallion",
#         default_args=default_args,
#         schedule="@daily",
#         catchup=False,
#         max_active_tasks=3,   # Одновременно в системе будет работать максимум 3 квадратика
#         max_active_runs=1,    # Если запустится ручной и автоматический ран, они будут стоять в очереди и выполняться СТРОГО по очереди, а не вместе
#         tags=["taxi", "postges", "pandas"]
# ) as dag:
#     # ---------------------------------------------------------
#     # ЭТАП 1: EXTRACT (Бронза — скачиваем сырые CSV на диск)
#     # ---------------------------------------------------------
#     t_ext_customers = PythonOperator(task_id="ext_customers", python_callable=extract.extract_customers)
#     t_ext_drivers = PythonOperator(task_id="ext_drivers", python_callable=extract.extract_drivers)
#     t_ext_locations = PythonOperator(task_id="ext_locations", python_callable=extract.extract_locations)
#     t_ext_tariffs = PythonOperator(task_id="ext_tariffs", python_callable=extract.extract_tariffs)
#     t_ext_statuses = PythonOperator(task_id="ext_statuses", python_callable=extract.extract_statuses)
#     t_ext_orders = PythonOperator(task_id="ext_orders", python_callable=extract.extract_orders)
#
#     # ---------------------------------------------------------
#     # ЭТАП 2: TRANSFORM (Серебро — типизируем и чистим строки)
#     # ---------------------------------------------------------
#     t_tr_customers = PythonOperator(task_id="tr_customers", python_callable=transform.process_customers_task)
#     t_tr_drivers = PythonOperator(task_id="tr_drivers", python_callable=transform.process_drivers_task)
#     t_tr_locations = PythonOperator(task_id="tr_locations", python_callable=transform.process_locations_task)
#     t_tr_tariffs = PythonOperator(task_id="tr_tariffs", python_callable=transform.process_tariffs_task)
#     t_tr_statuses = PythonOperator(task_id="tr_statuses", python_callable=transform.process_statuses_task)
#     t_tr_orders = PythonOperator(task_id="tr_orders", python_callable=transform.process_orders_task)
#
#     # ---------------------------------------------------------
#     # ЭТАП 3: LOAD (Золото — инжектим в СУБД с маппингом ключей)
#     # ---------------------------------------------------------
#     t_ld_customers = PythonOperator(task_id="ld_customers", python_callable=load_core.load_customers_task)
#     t_ld_drivers = PythonOperator(task_id="ld_drivers", python_callable=load_core.load_drivers_task)
#     t_ld_locations = PythonOperator(task_id="ld_locations", python_callable=load_core.load_locations_task)
#     t_ld_tariffs = PythonOperator(task_id="ld_tariffs", python_callable=load_core.load_tariffs_task)
#     t_ld_statuses = PythonOperator(task_id="ld_statuses", python_callable=load_core.load_statuses_task)
#     t_ld_orders = PythonOperator(task_id="ld_orders", python_callable=load_core.load_orders_task)
#
#     # ---------------------------------------------------------
#     # ЭТАП 4: Создаем таски для расчета витрин
#     # ---------------------------------------------------------
#     t_build_drivers_mart = PythonOperator(
#         task_id="build_drivers_efficiency_mart",
#         python_callable=load_core.build_drivers_efficiency_mart_task
#     )
#     t_build_customers_mart = PythonOperator(
#         task_id="build_customer_analytics_mart",
#         python_callable=load_core.build_customer_analytics_mart_task
#     )
#     t_build_sla_mart = PythonOperator(
#         task_id="build_orders_sla_mart",
#         python_callable=load_core.build_orders_sla_mart_task
#     )
#
#     # ---------------------------------------------------------
#     # НАСТРОЙКА ГРАФА ЗАВИСИМОСТЕЙ (LINEAGE)
#     # ---------------------------------------------------------
#
#     # Справочники обрабатываются строго последовательно по этапам
#     t_ext_customers >> t_tr_customers >> t_ld_customers
#     t_ext_drivers >> t_tr_drivers >> t_ld_drivers
#     t_ext_locations >> t_tr_locations >> t_ld_locations
#     t_ext_tariffs >> t_tr_tariffs >> t_ld_tariffs
#     t_ext_statuses >> t_tr_statuses >> t_ld_statuses
#
#     # Заказы тоже проходят свои этапы
#     t_ext_orders >> t_tr_orders
#
#     # 🧠 ГЛАВНАЯ СВЯЗКА (То, о чем мы говорили):
#     # Финальная загрузка заказов (ld_orders) ЖДЕТ, пока загрузятся ВСЕ справочники,
#     # а также пока подготовятся очищенные данные самих заказов (tr_orders)
#     [
#         t_ld_customers,
#         t_ld_drivers,
#         t_ld_locations,
#         t_ld_tariffs,
#         t_ld_statuses,
#         t_tr_orders
#     ] >> t_ld_orders
#     t_ld_orders >> [t_build_drivers_mart, t_build_customers_mart, t_build_sla_mart]
