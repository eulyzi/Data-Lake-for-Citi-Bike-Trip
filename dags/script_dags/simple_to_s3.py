from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def _local_to_s3(filename, bucket_name, key):
    """
    upload local file to S3
    param:
        filename(str): local file path need to upload
        bucket_name(str): bucket name of S3
        key(str):  key of S3
    return:
        None
    """
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, key=key, replace=True)
