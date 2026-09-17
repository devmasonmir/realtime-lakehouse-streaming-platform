from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'mason-mir',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'delta_lakehouse_compaction_and_vacuum',
    default_args=default_args,
    description='Nightly Delta Lake compaction and vacuum DAG',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    t_optimize = BashOperator(
        task_id='optimize_silver_delta_tables',
        bash_command='echo "Executing OPTIMIZE delta.`/mnt/silver/operational_events` ZORDER BY (event_type, timestamp_ms);"'
    )
    t_vacuum = BashOperator(
        task_id='vacuum_stale_files',
        bash_command='echo "Executing VACUUM delta.`/mnt/silver/operational_events` RETAIN 168 HOURS;"'
    )
    t_optimize >> t_vacuum
