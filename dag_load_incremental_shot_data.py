from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from load_incremental_shot_data import load_incremental_shot_data
import psycopg2
from psycopg2.pool import SimpleConnectionPool
import logging
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def check_load_status(table_name, **kwargs):
    """Проверяет, нужно ли загружать данные для таблицы."""
    pg_pool = SimpleConnectionPool(
        minconn=1, maxconn=4,
        dbname="nba_stats", user="admin", password="admin", host="postgres_db1", port="5432"
    )
    start_year = 2010
    end_year = 2023

    try:
        conn = pg_pool.getconn()
        cur = conn.cursor()
        cur.execute(f"""
            SELECT last_loaded_year
            FROM tech.load_progress
            WHERE table_name = '{table_name}'
        """)
        result = cur.fetchone()
        last_year = result[0] if result else start_year - 1
        next_year = last_year + 1
        cur.close()
        pg_pool.putconn(conn)

        if next_year > end_year:
            logger.info(f"Все годы для {table_name} загружены")
            return None

        csv_path = f"/opt/airflow/data/csv/NBA_{next_year}_Shots.csv"
        if not os.path.exists(csv_path):
            logger.info(f"Файл {csv_path} не найден")
            return None

        logger.info(f"Следующий год для загрузки {table_name}: {next_year}")
        kwargs['ti'].xcom_push(key=f'next_year_{table_name}', value=next_year)
        return next_year
    except psycopg2.Error as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        raise
    finally:
        pg_pool.closeall()

def update_load_progress(table_name, **kwargs):
    """Обновляет последний загруженный год в tech.load_progress."""
    pg_pool = SimpleConnectionPool(
        minconn=1, maxconn=4,
        dbname="nba_stats", user="admin", password="admin", host="postgres_db1", port="5432"
    )
    end_year = 2023
    next_year = kwargs['ti'].xcom_pull(key=f'next_year_{table_name}')

    if next_year is None:
        logger.info(f"Обновление для {table_name} не требуется")
        return

    try:
        conn = pg_pool.getconn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tech.load_progress (table_name, last_loaded_year, is_completed, load_timestamp)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (table_name)
            DO UPDATE SET last_loaded_year = %s, is_completed = %s, load_timestamp = CURRENT_TIMESTAMP
        """, (table_name, next_year, next_year >= end_year, next_year, next_year >= end_year))
        conn.commit()
        cur.close()
        pg_pool.putconn(conn)
        logger.info(f"Обновлен last_loaded_year до {next_year} для {table_name}")
    except psycopg2.Error as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        raise
    finally:
        pg_pool.closeall()

with DAG(
    'dag_load_incremental_shot_data',
    description='DAG для переноса данных о бросках из CSV-файлов в stage.',
    start_date=datetime(2025, 5, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:
    start = DummyOperator(task_id='start')
    check_status = PythonOperator(
        task_id='check_load_status_shot_data',
        python_callable=check_load_status,
        op_args=['shot_data'],
        provide_context=True
    )
    load_task = PythonOperator(
        task_id='load_data_shot_data',
        python_callable=load_incremental_shot_data,
        op_args=['/opt/airflow/data/csv/NBA_{{ ti.xcom_pull(key="next_year_shot_data") }}_Shots.csv', '{{ ti.xcom_pull(key="next_year_shot_data") }}', 'dag_load_incremental_shot_data', 'load_data_shot_data'],
        provide_context=True
    )
    update_progress = PythonOperator(
        task_id='update_progress_shot_data',
        python_callable=update_load_progress,
        op_args=['shot_data'],
        provide_context=True
    )
    trigger_next = TriggerDagRunOperator(
        task_id='trigger_dag_load_dds',
        trigger_dag_id='dag_load_dds'
    )
    end = DummyOperator(task_id='end')

    start >> check_status >> load_task >> update_progress >> trigger_next >> end
