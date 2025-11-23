from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from getparamoperator import PushDagRunConfigOperator


with DAG(
    dag_id='check_custom_opertor',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:
    custom_operator = PushDagRunConfigOperator(
        task_id='check_custom_operator',
    )
    custom_operator