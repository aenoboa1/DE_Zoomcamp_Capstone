# Data ingestion DAG for crime data
# https://dev.socrata.com/docs/queries/index.html


import os 
import logging

import requests
import pandas as pd

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def fetch_data(month,year):
    # EXTRACT DATA PER MONTH AND YEAR 
    
    # Note: the SODA API has a limit of 1000 calls , we can increase those using the $limit parameter

    url_response = requests.get(f"https://data.cityofchicago.org/resource/crimes.json?$where=date_extract_m(date)='{month}' and date_extract_y(date)='{year}'&$limit=100000000")
    print(url_response)
    
    
    file_name = 'chicago_crime_data_' + year + '-' + month + '.parquet' 
    path = f'{AIRFLOW_HOME}/{year}/' 
    print(path)
    
    try:
        os.mkdir(path)
    except OSError as error:
        print(error)   
    
    print("url response is %0.2d",url_response)
 
    # IF SUCCESSFUL
    if (url_response.status_code == 200):            
        print('Response code 200, Fetching Data ....')
        data = url_response.json()
        data_transformed = []
        if data:
            for columns in data:
                data_transformed.append(columns)
            df= pd.DataFrame(data_transformed)
            df.to_parquet(path+file_name,engine='pyarrow',index=False)
            print(f"Parquet: {file_name}")
            print(df.head())
            print(f"*** SAVED DATA FOR {year}-{month}")

    elif (url_response == 400):
        raise ValueError(f'Data for {month}-{year} not fetched.')
        return -1




