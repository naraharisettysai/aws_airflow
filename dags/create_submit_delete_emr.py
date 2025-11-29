from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
# from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3, time
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'email': [''],
    'depends_on_past': False,
    'catchup' : False,
    'start_date': datetime(2024, 4, 6),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
def create_emr_cluster():
    # Create an EMR cluster using Boto3

    emr_client = boto3.client('emr', region_name='eu-north-1')

    # Specify the configurations for your EMR cluster
    cluster_config = {
        'Name': 'retail-dataset-load-emr',
        'LogUri': 's3://aws-logs-323399920088-eu-north-1/elasticmapreduce/',
        # S3 bucket where EMR logs will be stored
        'ReleaseLabel': 'emr-6.9.0',  # EMR release version
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'MasterInstanceGroup',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'CoreInstanceGroup',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,  # Keep cluster alive even after all steps are completed
            'TerminationProtected': False,  # Allow cluster termination
            'Ec2KeyName': 'emr-key-pair',  # EC2 key pair for SSH access
        },
        'Applications': [
            {'Name': 'Spark'},  # Include Spark application
            {'Name': 'Hadoop'},  # Include Hadoop application
            # Add other applications as needed
        ],
        'VisibleToAllUsers': True,  # Cluster visibility
        'JobFlowRole': 'AmazonEMR-ServiceRole-20251029T134030',  # IAM role for EMR to access AWS services
        'ServiceRole': 'arn:aws:iam::323399920088:role/service-role/AmazonEMR-ServiceRole-20251029T134030',
        # IAM role for EMR service
    }

    response = emr_client.run_job_flow(**cluster_config)

    cluster_id = response['JobFlowId']

    # Print the cluster ID
    print("EMR Cluster ID:", response['JobFlowId'])

    # Specify the cluster ID you want to check
    EMRJobFlowId = str(response['JobFlowId'])

    # Describe the cluster
    response = emr_client.describe_cluster(ClusterId=EMRJobFlowId)

    # Get the state of the cluster
    cluster_state = response['Cluster']['Status']['State']

    while cluster_state != 'WAITING':
        print("Current cluster state : {}".format(cluster_state))
        print("The cluster is not created yet. Will check cluster status after 2 mins")
        time.sleep(120)
        response = emr_client.describe_cluster(ClusterId=EMRJobFlowId)
        cluster_state = response['Cluster']['Status']['State']
        print("Cluster state after 2 mins : {}".format(cluster_state))

    return str(cluster_id)
    
def check_step_status(cluster_id, step_id):
    emr_client = boto3.client('emr', region_name='eu-north-1')  # Replace 'your_region' with your AWS region
    while True:
        response = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = response['Step']['Status']['State']
        print("Step {} is currently in state: {}".format(step_id, state))
        if state in ['COMPLETED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(10)  # Wait for 10 seconds before checking again
        print("Wait for 10 seconds before checking again")

    
def run_spark_job(**kwargs):
    print("Inside spark job")
    # Retrieve the JobFlowId from the previous task's output
    job_flow_id = kwargs['ti'].xcom_pull(task_ids='create_emr_cluster')

    print("job_flow_id {}".format(job_flow_id))

    # Describe the cluster
    emr_client = boto3.client('emr', region_name='eu-north-1')
    response = emr_client.describe_cluster(ClusterId=job_flow_id)

    # Get the state of the cluster
    cluster_state = response['Cluster']['Status']['State']

    print(f"response : {response}")
    print(f"cluster_state : {cluster_state}")

    spark_steps = [
        {
            'Name': 'Spark Job Step',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode', 'cluster',
                         's3://growdata-de/s3_to_hive_spark.py']
            }
        }
    ]

    response = emr_client.add_job_flow_steps(JobFlowId=job_flow_id, Steps=spark_steps)

    print(response)

    step_id = response['StepIds'][0]

    print("Step added with ID:", step_id)

    # Check the status of the added step
    check_step_status(job_flow_id, step_id)

def terminate_emr_cluster(**kwargs):
    # Terminate a EMR cluster
    job_flow_id = kwargs['ti'].xcom_pull(task_ids='create_emr_cluster')
    # Terminate the EMR cluster using Boto3
    emr_client = boto3.client('emr', region_name='eu-north-1')
    emr_client.terminate_job_flows(JobFlowIds=[job_flow_id])
    
    
with DAG(
    'retail-dataset-load',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    process_etl = DummyOperator(task_id='process_etl', dag=dag)
    
    create_emr_task = PythonOperator(
    task_id='create_emr_cluster',
    python_callable=create_emr_cluster,
    dag=dag,
    )

    run_spark_job_task = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job,
    provide_context=True,
    dag=dag,
    )


    terminate_emr_task = PythonOperator(
    task_id='terminate_emr_cluster',
    python_callable=terminate_emr_cluster,
    dag=dag,
    )
    
    
    complete_etl = DummyOperator(task_id='complete_etl', dag=dag)
    
    
    process_etl >> create_emr_task >> run_spark_job_task >> terminate_emr_task >> complete_etl