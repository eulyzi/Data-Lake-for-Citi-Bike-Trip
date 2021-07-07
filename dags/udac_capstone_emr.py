from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
import operator
from datetime import datetime, timedelta

from script_operator.operator_data_upload import DataUploadOperator
from script_dags.crawl_to_local import stations_to_save, events_to_save, covids_to_save, weathers_to_save
from script_dags.clean_input_s3 import clean_input_data
from script_dags.simple_to_s3 import _local_to_s3

import os
import configparser

os_path = os.getcwd()
config = configparser.ConfigParser()
config.read(os_path+'/'+'airflow/dags/bks_aws.cfg')

job_flow_role = config['MAIN_PATH']['job_flow_role']
service_role = config['MAIN_PATH']['service_role']
BUCKET_NAME = config['MAIN_PATH']['bucket_name_s3']
local_data = config['MAIN_PATH']['staging_data_local']
local_script_elt = os_path + '/' + config['STAGE_SCRIPT']['local_elt_filename']
local_script_check = os_path + '/' + config['STAGE_SCRIPT']['local_elt_filename']
s3_script_elt = config['STAGE_SCRIPT']['s3_elt_filename']
s3_script_check = config['STAGE_SCRIPT']['s3_check_filename']
s3_inputs = config['MAIN_PATH']['input_key_s3']
s3_outputs = config['MAIN_PATH']['output_key_s3']

default_args = {
    'owner': 'ChenYue',
    'start_date': datetime(2020, 1, 1),
    'end_date': datetime(2020, 12, 31),
    # The DAG does not have dependencies on past runs
    'depends_on_past': False,
    # On failure, the task are retried 3 times
    'retries': 3,
    # Retries happen every 5 minutes
    'retry_delay': timedelta(minutes=5),
    # Catchup is turned off
    'catchup': False,
    # Do not email on retry
    'email_on_retry': False,
}

# Running spark on EMR with the help of
# https://www.startdataengineering.com/post/how-to-submit-spark-jobs-to-emr-cluster-from-airflow/
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#client
JOB_FLOW_OVERRIDES = {
    "Name": "test_emr_job_boto3",
    "LogUri": 's3://udacity-data-engineering-capstone/logs',
    "ReleaseLabel": "emr-6.3.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.large",
                "InstanceCount": 1,
            },
            {
                "Name": "Slave nodes",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m4.large",
                "InstanceCount": 1,
            },
        ],
        "Ec2KeyName": "AWS_EC2_Demo",
        "Ec2SubnetId": "subnet-d3dca48c",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "VisibleToAllUsers": True,
    "JobFlowRole": job_flow_role,
    "ServiceRole": service_role,
}

# Running spark on EMR with the help of
# https://www.startdataengineering.com/post/how-to-submit-spark-jobs-to-emr-cluster-from-airflow/
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#client
SPARK_STEPS = [
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/{{params.s3_inputs}}",
                "--dest=/hdfs_input",
            ],
        },
    },
    {
        "Name": "Run spark ELT",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script_elt }}",
            ],
        },
    },
    {
        "Name": "Run Data Quality Check",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script_check }}",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/hdfs_output",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_outputs }}",
            ],
        },
    },
]

with DAG(
        dag_id="udac_capstone_dags",
        default_args=default_args,
        schedule_interval="@monthly",
        max_active_runs=1,
) as dag:
    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
    # crawl data to local
    events_to_local = PythonOperator(
        dag=dag,
        task_id='events_to_local',
        python_callable=events_to_save,
        provide_context=True
    )
    weathers_to_local = PythonOperator(
        dag=dag,
        task_id='weathers_to_local',
        python_callable=weathers_to_save,
        provide_context=True,
        op_kwargs={'incremental_save': True}
    )
    covids_to_local = PythonOperator(
        dag=dag,
        task_id='covids_to_local',
        python_callable=covids_to_save,
        op_kwargs={'incremental_save': False},
    )
    stations_to_local = PythonOperator(
        dag=dag,
        task_id='stations_to_local',
        python_callable=stations_to_save,
        op_kwargs={'incremental_save': False},
    )
    # upload to s3
    events_to_s3 = DataUploadOperator(
        task_id='events_to_s3',
        dag=dag,
        glob_path=config['MAIN_PATH']['staging_data_local'] + config['STAGE_EVENTS']['events_mark'] + '*.csv',
        aws_conn_id='aws_default',
        bucket_name=BUCKET_NAME,
        key_prefix='inputs/' + config['STAGE_EVENTS']['events_mark'],
        replace=True,
        incremental_upload=True,
    )
    weathers_to_s3 = DataUploadOperator(
        task_id='weathers_to_s3',
        dag=dag,
        glob_path=config['MAIN_PATH']['staging_data_local'] + config['STAGE_WEATHERS']['weathers_mark'] + '*.json',
        aws_conn_id='aws_default',
        bucket_name=BUCKET_NAME,
        key_prefix='inputs/' + config['STAGE_WEATHERS']['weathers_mark'],
        replace=True,
        incremental_upload=True,
        gzip=False,
    )
    covids_to_s3 = DataUploadOperator(
        task_id='covids_to_s3',
        dag=dag,
        glob_path=config['MAIN_PATH']['staging_data_local'] + config['STAGE_COVIDS']['covids_mark'] + '*.csv',
        aws_conn_id='aws_default',
        bucket_name=BUCKET_NAME,
        key_prefix='inputs/' + config['STAGE_COVIDS']['covids_mark'],
        replace=True,
        incremental_upload=False,
    )
    stations_to_s3 = DataUploadOperator(
        task_id='stations_to_s3',
        dag=dag,
        glob_path=config['MAIN_PATH']['staging_data_local'] + config['STAGE_STATIONS']['stations_mark'] + '*.csv',
        aws_conn_id='aws_default',
        bucket_name=BUCKET_NAME,
        key_prefix='inputs/' + config['STAGE_STATIONS']['stations_mark'],
        replace=True,
        incremental_upload=False,
    )
    # upload script
    spark_elt_to_s3 = PythonOperator(
        dag=dag,
        task_id="spark_elt_to_s3",
        python_callable=_local_to_s3,
        op_kwargs={"filename": local_script_elt, "key": s3_script_elt, "bucket_name": BUCKET_NAME},
    )
    data_quality_to_s3 = PythonOperator(
        dag=dag,
        task_id="data_quality_to_s3",
        python_callable=_local_to_s3,
        op_kwargs={"filename": local_script_check, "key": s3_script_check, "bucket_name": BUCKET_NAME},
    )
    # Create an EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
        dag=dag,
    )
    # Add your steps to the EMR cluster
    step_adder = EmrAddStepsOperator(
        task_id="add_emr_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
        params={
            "BUCKET_NAME": BUCKET_NAME,
            "s3_inputs": s3_inputs,
            "s3_script_elt": s3_script_elt,
            "s3_script_check": s3_script_check,
            "s3_outputs": s3_outputs,
        },
        dag=dag,
    )
    last_step = len(SPARK_STEPS) - 1
    # wait for the steps to complete
    spark_elt_checker = EmrStepSensor(
        task_id="watch_ELT_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[1] }}",
        aws_conn_id="aws_default",
        dag=dag,
    )
    # data quality
    data_quality_checker = EmrStepSensor(
        task_id="watch_CHECK_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[3] }}",
        aws_conn_id="aws_default",
        dag=dag,
    )
    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        dag=dag,
    )
    # clean input data on s3
    clean_input = PythonOperator(
        task_id='clean_input',
        python_callable=clean_input_data,
        dag=dag,
        op_kwargs={'local_inputs': local_data,
                   'aws_conn_id': 'aws_default',
                   'bucket_name': BUCKET_NAME,
                   'prefix': s3_inputs,
                   }

    )
    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [events_to_local, weathers_to_local, covids_to_local, stations_to_local]
start_operator >> [spark_elt_to_s3, data_quality_to_s3]
events_to_local >> events_to_s3 >> create_emr_cluster
weathers_to_local >> weathers_to_s3 >> create_emr_cluster
covids_to_local >> covids_to_s3 >> create_emr_cluster
stations_to_local >> stations_to_s3 >> create_emr_cluster
spark_elt_to_s3 >> create_emr_cluster
data_quality_to_s3 >> create_emr_cluster
create_emr_cluster >> step_adder >> \
spark_elt_checker >> data_quality_checker >> terminate_emr_cluster >> clean_input >> end_operator
