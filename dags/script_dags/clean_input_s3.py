import shutil
import os
from airflow.hooks.S3_hook import S3Hook
from logging import info


def clean_input_local(local_inputs):
    """
    clean input files on local
    param:
        local_inputs(str): local folder or file path need to delete
    return:
        None
    """
    shutil.rmtree(local_inputs)
    os.makedirs(local_inputs)


def clean_input_s3(aws_conn_id, bucket_name, prefix):
    """
    clean input files on Amazon S3
    param:
        aws_conn_id(str): aws_conn_id in the Airflow
        bucket_name(str): Amazon S3 bucket name
        prefix(str): the prefix of Amazon S3 bucket
    return:
        None
    """
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    # find keys in bucket
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    # if keys is not None. Delete all the keys
    if keys:
        s3_hook.delete_objects(bucket=bucket_name, keys=keys)


def clean_input_data(local_inputs, aws_conn_id, bucket_name, prefix):
    """
    clean input files on local and  Amazon S3 by process 2 delete function
    param:
        local_inputs(str): local folder or file path need to delete
        aws_conn_id(str): aws_conn_id in the Airflow
        bucket_name(str): Amazon S3 bucket name
        prefix(str): the prefix of Amazon S3 bucket
    return:
        None
    """
    info("ready to clean")
    clean_input_local(local_inputs)
    info("local data cleaned")
    clean_input_s3(aws_conn_id, bucket_name, prefix)
    info("s3 data cleaned")
