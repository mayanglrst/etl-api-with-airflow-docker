# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import requests

url = 'https://api.sampleapis.com/simpsons/characters'

df = pd.DataFrame(requests.get(url).json())

df.reset_index(drop=True, inplace=True)

df.head()
df.tail()

import sqlalchemy
print(sqlalchemy.__version__)
