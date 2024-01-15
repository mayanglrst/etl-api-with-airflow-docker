#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 23:23:07 2023

@author: mayang
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
from sqlalchemy import create_engine

# Function to load data into postgresql
def load_data():
    # URL of the API - Extract
    url = 'https://api.sampleapis.com/simpsons/characters'

    # Making the API call - Extract
    response = requests.get(url).json()

    # Removing the 'id' field from each item in the response - Transform
    clean_table = [{key: value for key, value in item.items() if key != 'id'} for item in response]

    # Converting the clean table to a DataFrame - 3 - Load to staging
    df = pd.DataFrame(clean_table)

    # Database connection details for PostgreSQL - Load to Postgres DB
    username = 'postgres'
    password = '9009'
    host = 'localhost'
    port = '5433'
    database = 'thesimpsons'

    # Create a connection to the PostgreSQL database
    engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

    # Insert the DataFrame into PostgreSQL
    df.to_sql('simpsons', con=engine, if_exists='append', index=False)
    
    # Close the connection
    engine.dispose()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 17),  # Adjust the start date as required
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'load_simpsons_dag',
    default_args=default_args,
    description='A DAG to load simpsons api into postgresql database',
    schedule_interval=timedelta(days=1),  # Or use cron format '0 0 * * *'
)

# Define the task using PythonOperator
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Set the task in the DAG
load_data_task