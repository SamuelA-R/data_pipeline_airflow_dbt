import requests
import os
import json
import pandas as pd
import logging
from keys.api_key import get_api_key

API_KEY = get_api_key()

url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo'

r = requests.get(url, params=paramers)
data = r.json()

print(data)