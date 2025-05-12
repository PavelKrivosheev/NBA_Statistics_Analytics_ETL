from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from load_data_marts import load_data_mart

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'dag_data_marts',
    default_args=default_args,
    description='Load data marts from DDS',
    schedule_interval=None,  # Триггер от dag_load_dds
    start_date=datetime(2025, 5, 10),
    catchup=False,
) as dag:

    load_player_shot_efficiency = PythonOperator(
        task_id='load_player_shot_efficiency',
        python_callable=load_data_mart,
        op_kwargs={
            'table_name': 'player_shot_efficiency',
            'dag_id': 'dag_data_marts',
            'task_id': 'load_player_shot_efficiency'
        }
    )

    load_team_attack_efficiency = PythonOperator(
        task_id='load_team_attack_efficiency',
        python_callable=load_data_mart,
        op_kwargs={
            'table_name': 'team_attack_efficiency',
            'dag_id': 'dag_data_marts',
            'task_id': 'load_team_attack_efficiency'
        }
    )

    load_three_point_trends = PythonOperator(
        task_id='load_three_point_trends',
        python_callable=load_data_mart,
        op_kwargs={
            'table_name': 'three_point_trends',
            'dag_id': 'dag_data_marts',
            'task_id': 'load_three_point_trends'
        }
    )

    load_player_shot_efficiency >> load_team_attack_efficiency >> load_three_point_trends