from pyspark.sql import SparkSession
import operator


def create_spark_session():
    """
    set up a spark session.
    params:
        None
    return:
        spark(SparkSession): SparkSession
    """
    spark = SparkSession \
        .builder \
        .appName("Udacity Data Engineering Capstone") \
        .getOrCreate()
    return spark


def table_validate(spark, check_path, check_type, table_name, check_sql, expected_result, comparison):
    """
    check whether the data passes certain `check_type`.
    params:
        spark (SparkSession): SparkSession
        check_path(str): hdfs path where contains check files.
        check_type (str): data validate type name.
        table_name(str): table name needed to check
        check_sql (str): sql used to validate the target table
        expected_result (int): after run the sql, the expected returned rows.
        comparison (operator.function):
            python operator.function, used to do comparison between sql returns and expected_result.
    return:
        None
    """
    validation_path = check_path + '/' + table_name + '.parquet'
    validation_data = spark.read.parquet(validation_path)
    validation_data.createOrReplaceTempView(table_name)
    records = spark.sql(check_sql).take(1)
    # check results
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"{check_type} check failed.\n '{check_sql}' returned no results. ")
    num_records = records[0][0]
    is_check_pass = comparison(num_records, expected_result)
    if not is_check_pass:
        raise ValueError(f"{check_type} check failed.\n "
                         f"'{check_sql}' returned {num_records} rows,\n "
                         f"expected {str(comparison)} {expected_result} rows")


def main():
    """
    execute all the functions to build a data lake on AMS s3.
    - create spark session
    - process  fucntion `table_validate()`
    params:
        None
    return:
        None
    """
    spark = create_spark_session()
    output = '/hdfs_output'
    # check whether the parquet has result
    tables_check = [
        {'check_type': 'is_null','table_name': 'bikeshare_fact_table',
         'check_sql': "SELECT COUNT(*) FROM bikeshare_fact_table WHERE id IS NULL",
         'expected_result': 0,
         'comparison': operator.eq},
        {'check_type': 'is_empty', 'table_name': 'bikeshare_fact_table',
         'check_sql': "SELECT COUNT(*) FROM bikeshare_fact_table", 'expected_result': 0,
         'comparison': operator.gt},
        {'check_type': 'is_empty', 'table_name': 'dim_weather_table',
         'check_sql': "SELECT COUNT(*) FROM dim_weather_table", 'expected_result': 0,
         'comparison': operator.gt},
        {'check_type': 'is_empty', 'table_name': 'dim_covid_table',
         'check_sql': "SELECT COUNT(*) FROM dim_covid_table", 'expected_result': 0,
         'comparison': operator.gt},
        {'check_type': 'is_empty', 'table_name': 'dim_user_agg_table',
         'check_sql': "SELECT COUNT(*) FROM dim_user_agg_table", 'expected_result': 0,
         'comparison': operator.gt},
        {'check_type': 'is_empty', 'table_name': 'dim_time_table',
         'check_sql': "SELECT COUNT(*) FROM dim_time_table", 'expected_result': 0,
         'comparison': operator.gt},
        {'check_type': 'is_empty', 'table_name': 'dim_bike_table',
         'check_sql': "SELECT COUNT(*) FROM dim_bike_table", 'expected_result': 0,
         'comparison': operator.gt}]
    for check in tables_check:
        table_validate(spark, check_path=output, **check)


if __name__ == "__main__":
    main()
