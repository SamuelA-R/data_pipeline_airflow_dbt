import requests
import os
import json
import pandas as pd
import logging
from keys.api_key import get_api_key

API_KEY = get_api_key()

bronze = os.getenv('BRONZE', 'C:\\projetos-paralelos-samuel\\data-pipeline-dbt-airflow\\data\\bronze')
url = 'https://www.alphavantage.co/query'

TICKERS = [
    "AAPL", "GOOGL", "MSFT",  # Ações estrangeiras
    "PETR4.SA", "VALE3.SA", "ITUB4.SA"  # Ações brasileiras
]

def fetch_stock_data(TICKERS):

    for ticker in TICKERS:
        paramers = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': ticker,
            'outputsize': 'compact',  # or 'full'
            'datatype': 'json',
            'apikey': API_KEY
        }
        r = requests.get(url, params=paramers)
        data = r.json()

        print(data)

        answer = requests.get(url=url, params=paramers)

        if r.status_code == 200:
            dados = r.json()
            nome_arquivo = f"{ticker}.json"
            caminho_arquivo = os.path.join(bronze, nome_arquivo)
            with open(caminho_arquivo, 'w') as arquivo:
                json.dump(dados, arquivo)
            
            print(f"Dados ticker {ticker} salvo em {caminho_arquivo}")

fetch_stock_data(TICKERS)