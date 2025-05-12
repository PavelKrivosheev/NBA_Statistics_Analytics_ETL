from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from load_dds_tables import load_dds_table
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/dag_load_dds.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

with DAG(
    dag_id='dag_load_dds',
    description='DAG для загрузки данных из stage в DDS',
    start_date=datetime(2025, 5, 10),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:
    # Начальная задача
    start = DummyOperator(task_id='start')

    # Создание задач для загрузки таблиц
    tasks = {}
    for table_name in ["dim_time", "dim_zone", "dim_team", "dim_player", "dim_game", "fact_shots", "fact_games"]:
        tasks[table_name] = PythonOperator(
            task_id=f'load_{table_name}',
            python_callable=load_dds_table,
            op_args=[table_name, 'dag_load_dds', f'load_{table_name}'],
            provide_context=True
        )

    # Задача для триггера следующего DAG
    trigger_next = TriggerDagRunOperator(
        task_id='trigger_dag_data_marts',
        trigger_dag_id='dag_data_marts'
    )

    # Конечная задача
    end = DummyOperator(task_id='end')

    # Определение зависимостей
    start >> tasks["dim_time"] >> tasks["dim_zone"] >> tasks["dim_team"] >> tasks["dim_player"] >> tasks["dim_game"] >> [tasks["fact_shots"], tasks["fact_games"]] >> trigger_next >> end

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# from load_dds_tables import load_dds_table
#
# # Определение DAG
# dag = DAG(
#     dag_id='dag_load_dds',
#     description='DAG для загрузки данных из stage в DDS',
#     schedule_interval=None,
#     start_date=datetime(2025, 5, 10),
#     catchup=False
# )
#
# # Создание задач
# tasks = {}
# for table_name in ["dim_time", "dim_zone", "dim_team", "dim_player", "dim_game", "fact_shots", "fact_games"]:
#     tasks[table_name] = PythonOperator(
#         task_id=f'load_{table_name}',
#         python_callable=load_dds_table,
#         op_args=[table_name, 'dag_load_dds', f'load_{table_name}'],
#         dag=dag
#     )
#
# # Определение зависимостей
# tasks["dim_time"] >> tasks["dim_zone"] >> tasks["dim_team"] >> tasks["dim_player"] >> tasks["dim_game"] >> [tasks["fact_shots"], tasks["fact_games"]]