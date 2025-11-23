from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def print_params(**kwargs):
    conf = kwargs.get('dag_run').conf
    get_name=conf.get("name")
    task_instance=kwargs["task_instance"]
    print(get_name)
    task_instance.xcom_push(key="name", value=get_name)

    print("Received params:", conf)
def get_xcom(**kwargs):
    task_instance=kwargs["task_instance"]
    get_name=task_instance.xcom_pull(task_ids="print_params", key="name")
    print("get_xcom_value",get_name)


with DAG(
    dag_id='example_conf_dag',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:
    print_task = PythonOperator(
        task_id='print_params',
        python_callable=print_params,
        provide_context=True
    )
    get_xcom_values = PythonOperator(
        task_id='get_xcom',
        python_callable=get_xcom,
        provide_context=True
    )
    print_task>>get_xcom_values
