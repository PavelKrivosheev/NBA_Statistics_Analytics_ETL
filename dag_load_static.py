from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from load_static_tables import load_static_table

with DAG(
    'dag_load_static',
    description='DAG для переноса данных из источников в stage.',
    start_date=datetime(2025, 5, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1
) as dag:
    start = DummyOperator(task_id='start')
    trigger_game = TriggerDagRunOperator(
        task_id='trigger_dag_load_incremental_game',
        trigger_dag_id='dag_load_incremental_game'
    )
    end = DummyOperator(task_id='end')

    static_tables = [
        'common_player_info', 'other_stats', 'player', 'team', 'team_details',
        'draft_history', 'draft_combine_stats'
    ]
    load_tasks = []
    for table_name in static_tables:
        load_task = PythonOperator(
            task_id=f'load_{table_name}',
            python_callable=load_static_table,
            op_args=[table_name, '/opt/airflow/data/nba.sqlite', 'dag_load_static', f'load_{table_name}'],
            provide_context=True
        )
        load_tasks.append(load_task)

    start >> load_tasks >> trigger_game >> end