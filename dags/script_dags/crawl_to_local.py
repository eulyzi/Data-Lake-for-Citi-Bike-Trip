import os
import configparser
import requests
import pandas as pd
import zipfile
import io
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
from logging import info

os_path = os.getcwd()
config = configparser.ConfigParser()
config.read(os_path+'/'+'airflow/dags/bks_aws.cfg')


def stations_to_save(incremental_save=True):
    """
    crawl bike trip station info to local
    param
        incremental_save(bool): crawl and overwrite the existing file if set to be False, else do not crawl
    return:
        None
    """
    folder_local = config['MAIN_PATH']['staging_data_local'] + config['STAGE_STATIONS']['stations_mark']
    if not os.path.exists(folder_local):
        os.makedirs(folder_local)
    # check the existence of file, if not fetch data
    file_local = folder_local + 'staging_stations.csv'
    if (not os.path.exists(file_local)) or (not incremental_save):
        info("Fetch data")
        stage_stations_url = config['STAGE_STATIONS']['stage_stations_url']
        resp = requests.get(stage_stations_url)
        resp_json = resp.json()
        stations_json = resp_json['data']['stations']
        df = pd.DataFrame.from_dict(stations_json)
        df.to_csv(file_local)
    else:
        info("Data existed, pass")
        pass


def events_to_save(**kwargs):
    """
    crawl Citi Bike Trip History to local
    param
        kwargs(dict): used to get execution_date of Airflow.
    return:
        None
    """
    folder_local = config['MAIN_PATH']['staging_data_local'] + config['STAGE_EVENTS']['events_mark']
    if not os.path.exists(folder_local):
        os.makedirs(folder_local)
    execution_date = kwargs['execution_date']
    execution_yyyymm = execution_date.strftime('%Y%m')
    # fetch history records of tripdate
    info("fetch history records of nyc")
    url_list = [
        config['STAGE_EVENTS']['stage_events_nyc_url'],
        config['STAGE_EVENTS']['stage_events_jc_url']
    ]
    for url in url_list:
        url_add_date = url.format(year_month=execution_yyyymm)
        info(f"Fetch {url_add_date}")
        r = requests.get(url_add_date)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(folder_local)


def covids_to_save(incremental_save=True):
    """
    crawl covids cases daily report of NYC to local,
    and merge the complementary file (0 cases before the first reported day)
    param
        incremental_save(bool): crawl and overwrite the existing file if set to be False, else do not crawl
    return:
        None
    """
    folder_local = config['MAIN_PATH']['staging_data_local'] + config['STAGE_COVIDS']['covids_mark']
    if not os.path.exists(folder_local):
        os.makedirs(folder_local)
    # check the existence of file if not fetch
    file_local = folder_local + 'staging_covids.csv'
    if (not os.path.exists(file_local)) or (not incremental_save):
        info("Fetch data")
        source_github = config['STAGE_COVIDS']['stage_covids_url']
        df_data = pd.read_csv(source_github)
        # read complement file
        source_complement = os.getcwd() + '/' + config['STAGE_COVIDS']['stage_covids_complement']
        df_complement = pd.read_csv(source_complement)
        # merge `df_data` and `df_complement`
        df = df_complement.append(df_data)
        df.to_csv(file_local)
    else:
        info("Data existed, pass")
        pass


def weathers_to_save(incremental_save=True, **kwargs):
    """
    crawl Hourly Weather report of NYC to local
    param
        incremental_save(bool): crawl and overwrite the existing file if set to be False, else do not crawl
        kwargs(dict): used to get execution_date of Airflow.
    return:
        None
    """
    folder_local = config['MAIN_PATH']['staging_data_local'] + '/' + config['STAGE_WEATHERS']['weathers_mark']
    if not os.path.exists(folder_local):
        os.makedirs(folder_local)
    # ready to fetch weather data by api
    api_key = config['STAGE_WEATHERS']['api_key']
    api_url = config['STAGE_WEATHERS']['stage_weather_url']
    execution_date = kwargs['execution_date']
    start = execution_date - relativedelta(days=2)
    end = execution_date + relativedelta(months=1, days=2)
    weather_date = start
    while weather_date <= end:
        request_date = weather_date.strftime('%Y%m%d')
        file_local = folder_local + f"weather_{request_date}.json"
        info(request_date)
        # confirm the existence of file path
        if (not os.path.exists(file_local)) or (not incremental_save):
            info("Fetch data")
            url = api_url.format(api_key=api_key, request_date=request_date)
            resp = requests.get(url)
            resp_json = resp.json()
            weather_json = resp_json['observations']
            # save results
            with open(file_local, 'w') as f:
                json.dump(weather_json, f)
            weather_date += timedelta(days=1)
            time.sleep(5)
        else:
            info("Data existed, pass")
            weather_date += timedelta(days=1)
            pass
