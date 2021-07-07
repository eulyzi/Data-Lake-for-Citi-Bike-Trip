from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import re
from airflow.exceptions import AirflowException
import glob
from collections import Iterable


class DataUploadOperator(BaseOperator):
    """
    upload local folder to s3
    params:
        glob_path (str): local folder needed to upload to S3.
        aws_conn_id(str):aws_conn_id
        bucket_name(str):bucket name
        key_prefix(str):prefix of bucket
        replace(str):
            flag to decide whether or not to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
        incremental_upload(bool): if set True, only upload files which are not exisited on S3.
        gzip(bool):if set True,gzip file before uploading.
    return:
        None
    """
    ui_color = '#89DA59'

    def __init__(self,
                 glob_path,
                 aws_conn_id,
                 bucket_name='',
                 key_prefix='',
                 replace=True,
                 incremental_upload=True,
                 gzip=True,
                 *args, **kwargs):
        super(DataUploadOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.glob_path = glob_path
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix
        self.replace = replace
        self.incremental_upload = incremental_upload
        self.gzip = gzip

    def _list_s3(self, bucket_name, prefix='', delimiter='',
                 page_size=None, max_items=None):
        """
        Lists keys in a bucket under prefix and not containing delimiter
        param:
            bucket_name(str): the name of the bucket
            prefix(str): a key prefix
            delimiter(str): the delimiter marks key hierarchy.
            page_size(int):  pagination size
            max_items(int): maximum items to return
        return:
            key_list(list): keys on requested buket and prefix
        """
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        key_list = s3_hook.list_keys(bucket_name, prefix=prefix, delimiter=delimiter,
                                     page_size=page_size, max_items=max_items)
        return key_list

    def _local_to_s3(self, filename, bucket_name, key, replace=True, gzip=True):
        """
        Loads a local file to S3
        param:
            filename(str): name of the file to load.
            bucket_name(str): Name of the bucket in which to store the file
            key(str): S3 key that will point to the file
            replace(bool):  flag to decide whether or not to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
            gzip(bool): If True, the file will be compressed locally
        return:
            None
        """
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_hook.load_file(filename=filename, bucket_name=bucket_name, key=key, replace=replace, gzip=gzip)

    def _extract_filename(self, path_string):
        """
        Extracct filename and filetype.
        If the path_string is 'D:\123.csv', the return would be '123.csv'.
        Because the function parse filepath from local path and s3 and local file may be uploaded into a '123.csv.gz',
        style.local '123.csv' equals  s3 '123.csv.gz'. The function replace '.gz' with ''.
        param:
            path_string(str): local path of file
        return:
            filename_in_s3(str): string like {filename}.{filetype}
        """
        # delete .gz
        path_string = path_string.replace('.gz', '')
        # check has . filetype
        filetype_check = re.findall('^.*?\.\w+$', path_string)
        if filetype_check:
            parsed = re.findall('/([a-zA-Z0-9-_]+\.\w+$)', path_string)
            if parsed:
                filename_in_s3 = parsed[0]
            else:
                self.log.info(f"Filename can not be parsed from {path_string}")
                raise AirflowException(
                    f"Filename can not be parsed from {path_string}")
            return filename_in_s3

    def execute(self, context):
        """
        execute the operator
        param:
            context: the same dictionary used as when rendering jinja templates
        return:
            None
        """
        self.log.info(f'{self.glob_path} not implemented yet')
        local_list = glob.glob(self.glob_path)
        self.log.info(local_list)
        self.log.info('check local file needed to be uploaded')
        if self.incremental_upload:
            local_set = set(map(self._extract_filename, local_list))
            key_list = self._list_s3(bucket_name=self.bucket_name, prefix=self.key_prefix)
            print(key_list)
            print(type(key_list))
            key_set = set(map(self._extract_filename, key_list)) if isinstance(key_list, Iterable) else set()
            file_to_upload = local_set - key_set
        else:
            file_to_upload = set(map(self._extract_filename, local_list))
        self.log.info(f'{str(file_to_upload)} is ready to upload')
        for filename in local_list:
            key_filename = self._extract_filename(filename)
            if key_filename in file_to_upload:
                if self.gzip:
                    path_file_s3 = self.key_prefix + key_filename + '.gz'
                else:
                    path_file_s3 = self.key_prefix + key_filename
                self._local_to_s3(filename=filename, bucket_name=self.bucket_name, key=path_file_s3,
                                  gzip=self.gzip)
                self.log.info(f'{key_filename} is uploaded')
        self.log.info(f"All the check passed")
