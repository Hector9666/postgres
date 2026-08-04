import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Гарантируем, что Airflow видит наши модули в папке dags
sys.path.append(os.path.dirname(__file__))

# Импортируем задачи из ваших адаптированных модулей
import extract, transform, load_core

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 8, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        dag_id="taxi_etl_medallion",
        default_args=default_args,
        schedule="@daily",
        catchup=False,
        max_active_tasks=3,   # Одновременно в системе будет работать максимум 3 квадратика
        max_active_runs=1,    # Если запустится ручной и автоматический ран, они будут стоять в очереди и выполняться СТРОГО по очереди, а не вместе
        tags=["taxi", "postges", "pandas"]
) as dag:
    # ---------------------------------------------------------
    # ЭТАП 1: EXTRACT (Бронза — скачиваем сырые CSV на диск)
    # ---------------------------------------------------------
    t_ext_customers = PythonOperator(task_id="ext_customers", python_callable=extract.extract_customers)
    t_ext_drivers = PythonOperator(task_id="ext_drivers", python_callable=extract.extract_drivers)
    t_ext_locations = PythonOperator(task_id="ext_locations", python_callable=extract.extract_locations)
    t_ext_tariffs = PythonOperator(task_id="ext_tariffs", python_callable=extract.extract_tariffs)
    t_ext_statuses = PythonOperator(task_id="ext_statuses", python_callable=extract.extract_statuses)
    t_ext_orders = PythonOperator(task_id="ext_orders", python_callable=extract.extract_orders)

    # ---------------------------------------------------------
    # ЭТАП 2: TRANSFORM (Серебро — типизируем и чистим строки)
    # ---------------------------------------------------------
    t_tr_customers = PythonOperator(task_id="tr_customers", python_callable=transform.process_customers_task)
    t_tr_drivers = PythonOperator(task_id="tr_drivers", python_callable=transform.process_drivers_task)
    t_tr_locations = PythonOperator(task_id="tr_locations", python_callable=transform.process_locations_task)
    t_tr_tariffs = PythonOperator(task_id="tr_tariffs", python_callable=transform.process_tariffs_task)
    t_tr_statuses = PythonOperator(task_id="tr_statuses", python_callable=transform.process_statuses_task)
    t_tr_orders = PythonOperator(task_id="tr_orders", python_callable=transform.process_orders_task)

    # ---------------------------------------------------------
    # ЭТАП 3: LOAD (Золото — инжектим в СУБД с маппингом ключей)
    # ---------------------------------------------------------
    t_ld_customers = PythonOperator(task_id="ld_customers", python_callable=load_core.load_customers_task)
    t_ld_drivers = PythonOperator(task_id="ld_drivers", python_callable=load_core.load_drivers_task)
    t_ld_locations = PythonOperator(task_id="ld_locations", python_callable=load_core.load_locations_task)
    t_ld_tariffs = PythonOperator(task_id="ld_tariffs", python_callable=load_core.load_tariffs_task)
    t_ld_statuses = PythonOperator(task_id="ld_statuses", python_callable=load_core.load_statuses_task)
    t_ld_orders = PythonOperator(task_id="ld_orders", python_callable=load_core.load_orders_task)

    # ---------------------------------------------------------
    # ЭТАП 4: Создаем таски для расчета витрин
    # ---------------------------------------------------------
    t_build_drivers_mart = PythonOperator(
        task_id="build_drivers_efficiency_mart",
        python_callable=load_core.build_drivers_efficiency_mart_task
    )
    t_build_customers_mart = PythonOperator(
        task_id="build_customer_analytics_mart",
        python_callable=load_core.build_customer_analytics_mart_task
    )
    t_build_sla_mart = PythonOperator(
        task_id="build_orders_sla_mart",
        python_callable=load_core.build_orders_sla_mart_task
    )

    # ---------------------------------------------------------
    # НАСТРОЙКА ГРАФА ЗАВИСИМОСТЕЙ (LINEAGE)
    # ---------------------------------------------------------

    # Справочники обрабатываются строго последовательно по этапам
    t_ext_customers >> t_tr_customers >> t_ld_customers
    t_ext_drivers >> t_tr_drivers >> t_ld_drivers
    t_ext_locations >> t_tr_locations >> t_ld_locations
    t_ext_tariffs >> t_tr_tariffs >> t_ld_tariffs
    t_ext_statuses >> t_tr_statuses >> t_ld_statuses

    # Заказы тоже проходят свои этапы
    t_ext_orders >> t_tr_orders

    # 🧠 ГЛАВНАЯ СВЯЗКА (То, о чем мы говорили):
    # Финальная загрузка заказов (ld_orders) ЖДЕТ, пока загрузятся ВСЕ справочники,
    # а также пока подготовятся очищенные данные самих заказов (tr_orders)
    [
        t_ld_customers,
        t_ld_drivers,
        t_ld_locations,
        t_ld_tariffs,
        t_ld_statuses,
        t_tr_orders
    ] >> t_ld_orders
    t_ld_orders >> [t_build_drivers_mart, t_build_customers_mart, t_build_sla_mart]
