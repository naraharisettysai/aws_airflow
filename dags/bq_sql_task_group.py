from airflow import DAG
from datetime import date, timedelta
from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
import pendulum

DEFAULT_ARGS = {
    'owner': 'venkata',
    'depends_on_past': False,
    'retries': 0,
    'email': [],
    'email_on_retry': False,
    'email_on_failures': False,
}

@dag(
    dag_id="sample_dag_task_group",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.now("Canada/Eastern").subtract(days=1),
    schedule_interval=None,
    catchup=False,
    tags=["example"]
)
def dag_seq():
    dag_config = Variable.get("config_sample", deserialize_json=True)
    file_name = dag_config["filename"]
    transformations_sql = dag_config["tranformations"]

    @task_group(group_id="file_poke")
    def file_poke():
        sensors = []
        for i in file_name:
            sensor = GCSObjectsWithPrefixExistenceSensor(
                task_id=f"poke_{i['file_name']}",
                bucket=dag_config["landing_bucket"],
                prefix=f"files/{i['file_name']}",
                poke_interval=15.0,
                timeout=60 * 5,
                mode="poke"
            )
            sensors.append(sensor)
        return sensors

    dummy_task = EmptyOperator(task_id="dummy_task1")

    gcstobq = GCSToBigQueryOperator(
        task_id="gcstobq_task",
        bucket=dag_config["landing_bucket"],
        source_objects=["files/uber_data_*.csv"],
        destination_project_dataset_table=dag_config["destination_table_stg"],
        source_format="CSV",
        skip_leading_rows=1,
        quote_character=None,
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True,
        compression=None,
        field_delimiter=",",
        write_disposition='WRITE_TRUNCATE'
    )

    @task_group(group_id="execute_query")
    def execute_query():
        queries = []
        for i in transformations_sql:
            query = BigQueryInsertJobOperator(
                task_id=f"run_{i['sql'].replace('.sql', '')}",
                configuration={
                    "query": {
                        "query": f"{{% include '/sql/{i['sql']}' %}}",
                        "useLegacySql": False,
                        "destinationTable": {
                            "projectId": "dataflowpre",
                            "datasetId": i["destination_table"].split('.')[1],
                            "tableId": i["destination_table"].split('.')[-1]
                        },
                        "writeDisposition": "WRITE_TRUNCATE"
                    }
                },
                location="US"
            )
            queries.append(query)
        return queries

    file_poke() >> dummy_task >> gcstobq >> execute_query()

dag_seq()
