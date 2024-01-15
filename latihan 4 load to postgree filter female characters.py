#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 21:50:19 2023

@author: mayang
"""

import pandas as pd
from sqlalchemy import create_engine

# Database connection details for PostgreSQL - Load to Postgres DB
username = 'postgres'
password = '9009'
host = 'localhost'
port = '5433'
database = 'thesimpsons'

# Create a connection to the PostgreSQL database
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

# SQL query to select all female characters
query = "SELECT * FROM charsimpsons WHERE gender = 'f'"

# Execute the query and store the result in a DataFrame
df_f = pd.read_sql_query(query, engine)

# Insert the DataFrame into the 'charsimpsons_female' table
df_f.to_sql('charsimpsons_female', con=engine, if_exists='append', index=False)

# Close the connection
engine.dispose()
