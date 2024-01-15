#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 19:04:16 2023

@author: mayang
"""

import requests
import pandas as pd
from sqlalchemy import create_engine

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
df.to_sql('charsimpsons', con=engine, if_exists='append', index=False)
