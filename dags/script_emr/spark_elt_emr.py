from pyspark.sql import SparkSession


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


def process_data(spark, input_data, output_data):
    """
    process `song_data` into `songs` and `artists` table.
    params:
        spark (SparkSession): SparkSession
        input_data (str): file path for input data
        output_data(str): file path for output data
    return:
        None
    """
    # staging events
    stage_events_input = 'events' + '/*.csv.gz'
    stage_events_path = input_data + '/' + stage_events_input
    events_log = spark.read.csv(stage_events_path, header=True)
    new_column_name_list = list(map(lambda x: x.replace(" ", "_"), events_log.columns))
    events_log = events_log.toDF(*new_column_name_list)
    # create table view
    events_log.createOrReplaceTempView("events_log_table")

    # staging weathers
    stage_weathers_input = 'weathers' + '/*.json'
    stage_weathers_path = input_data + '/' + stage_weathers_input
    weathers_log = spark.read.json(stage_weathers_path)
    # create table view
    weathers_log.createOrReplaceTempView("weathers_log_table")

    # staging stations
    stage_stations_input = 'stations' + '/*.csv.gz'
    stage_stations_path = input_data + '/' + stage_stations_input
    stations_log = spark.read.csv(stage_stations_path, header=True)
    # create table view
    stations_log.createOrReplaceTempView("stations_log_table")

    # staging covids
    stage_covids_input = 'covids' + '/*.csv.gz'
    stage_covids_path = input_data + '/' + stage_covids_input
    covids_log = spark.read.csv(stage_covids_path, header=True)
    new_column_name_list = list(map(lambda x: x.lower(), covids_log.columns))
    covids_log = covids_log.toDF(*new_column_name_list)
    # create table view
    covids_log.createOrReplaceTempView("covids_log_table")

    # create fact table bikeshare_fact_table
    bikeshare_fact_table = spark.sql("""
    select
        md5(concat(starttime,bikeid)) as id,
        to_timestamp(starttime) as start_time,
        tripduration as duration,
        year(to_timestamp(starttime)) as year,
        month(to_timestamp(starttime)) as month,
        start_station_id,
        end_station_id,
        bikeid as bike_id,
        concat(usertype,gender,birth_year) as user_agg_id,
        to_date(co.date_of_interest,'MM/dd/yyyy') as covid_id,
        to_timestamp(valid_time_gmt)  as weather_id
    from events_log_table ev
    left join covids_log_table co ON 
        (to_date(ev.starttime) = to_date(co.date_of_interest,'MM/dd/yyyy'))
    left join weathers_log_table ON 
        (unix_seconds(to_timestamp(concat(substr(starttime,0,13),':00:00'))) -540) = valid_time_gmt
    order by starttime
    """)
    bikeshare_fact_table.write.parquet("{}/bikeshare_fact_table.parquet".format(output_data),
                                       partitionBy=["year", "month"], mode="append")

    # create dim time
    dim_time_table = spark.sql("""
    with time_temp  as 
    (select
        distinct to_timestamp(starttime) as start_time
    from events_log_table
    )
    select 
        dat,
        minute(start_time) as minute,
        hour(start_time) as hour, 
        day(start_time) as day, 
        weekofyear(start_time) as week, 
        month(start_time) as month, 
        year(start_time) as year, 
        dayofweek(start_time) as weekday
    from
         time_temp
    """)
    dim_time_table.write.parquet("{}/dim_time_table.parquet".format(output_data), partitionBy=["year", "month"],
                                 mode="append")

    # create dim user_agg
    dim_user_agg_table = spark.sql("""
    select
        distinct concat(usertype,gender,birth_year) as user_agg_id,
        usertype,
        gender,
        birth_year
    from events_log_table
    """)
    dim_user_agg_table.write.parquet("{}/dim_user_agg_table.parquet".format(output_data),
                                     partitionBy=["usertype", "birth_year"], mode="overwrite")

    # create_dim_bike
    dim_bike_table = spark.sql("""
    select
        distinct bikeid as bike_id
    from events_log_table
    order by bike_id
    """)
    dim_bike_table.write.parquet("{}/dim_bike_table.parquet".format(output_data), mode="overwrite")

    # create covid dim
    dim_covid_table = spark.sql("""
    select
        to_date(date_of_interest,'MM/dd/yyyy') as covid_id,
        bx_case_count,
        bx_probable_case_count,
        bk_case_count,
        bk_probable_case_count,
        mn_case_count,
        mn_probable_case_count,
        qn_case_count,
        qn_probable_case_count,
        si_case_count,
        si_probable_case_count,
        incomplete
    from covids_log_table
    """)
    dim_covid_table.write.parquet("{}/dim_covid_table.parquet".format(output_data), mode="overwrite")

    # create weather dim
    dim_weather_table = spark.sql("""
    select
        to_timestamp(valid_time_gmt) as weather_id,
        temp as temperature,
        dewPt as dew_point,
        rh as humidity,
        day_ind as wind,
        wspd as wind_speed,
        (case when gust is NULL THEN  0 else gust end) as wind_gust,
        pressure,
        precip_hrly as precip,
        wx_phrase as condition
    from weathers_log_table
    order by valid_time_gmt
    """)
    dim_weather_table.write.parquet("{}/dim_weather_table.parquet".format(output_data), mode="overwrite")

    # create station
    dim_station = spark.sql("""
    select
        Distinct station_id,
        external_id,
        name,
        short_name,
        region_id,
        legacy_id,
        station_type,
        lat as latitude,
        lon as longtitude,
        capacity,
        has_kiosk,
        electric_bike_surcharge_waiver,
        eightd_has_key_dispenser,
        rental_methods
    from stations_log_table
    order by station_id
    """)
    dim_station.write.parquet("{}/dim_station.parquet".format(output_data), mode="overwrite")


def main():
    """
    execute all the functions to build a data lake on AMS s3.
    - create spark session
    - process  fucntion `process_data()`
    params:
        None
    return:
        None
    """
    spark = create_spark_session()
    input_data="/hdfs_input"
    output_data="/hdfs_output"
    process_data(spark, input_data=input_data, output_data=output_data)


if __name__ == "__main__":
    main()
