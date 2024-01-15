#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 21:59:56 2023

@author: mayang
"""

import requests
import pandas as pd

# URL of the API
url = 'https://api.sampleapis.com/simpsons/characters'

# Making the API call - 1 Extract
response = requests.get(url).json()

# Removing the 'id' field from each item in the response - 2 Transform
clean_table = [{key: value for key, value in item.items() if key != 'id'} for item in response]

# Converting the clean table to a DataFrame - 3 -Load
df = pd.DataFrame(clean_table)

df.head()