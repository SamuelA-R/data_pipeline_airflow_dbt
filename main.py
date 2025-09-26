import requests
import os
import json
import pandas as pd
import logging
from keys.api_key import get_api_key
from datetime import datetime, timedelta

data_de_extração = datetime.now().strftime("%Y-%m-%d")
end_time = datetime.now()
start_time = (datetime.now() + timedelta(-1)).date()

API_KEY = os.getenv("API_KEY_STOCK")

bronze = os.getenv('BRONZE', 'C:\\projetos-paralelos-samuel\\data-pipeline-dbt-airflow\\data\\bronze')
url = 'https://www.alphavantage.co/query'

TICKERS = [
    "AAPL", "GOOGL", "MSFT",  # Ações estrangeiras
    "PETR4.SA", "VALE3.SA", "ITUB4.SA"  # Ações brasileiras
]

INDICADORES = [
    "EMA", "SMA", "CCI", "WMA", "DEMA", 
    "TEMA", "KAMA", "ADX", "RSI", 
    "WILLR", "OBV"
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
        else:
            print(f"Erro ao buscar dados do ticker {ticker}: {r.status_code} - {r.text}")

def dados_tesouro():
    os.makedirs(bronze, exist_ok=True)
    parameters = {
        "function": "TREASURY_YIELD",
        "interval": "daily",
        "apikey": API_KEY,
        "maturity":"2year",
        "interval":"daily"
    }

    r = requests.get(url, params=parameters)
    if r.status_code == 200:
        dados = r.json()
        nome_arquivo = f"tesouro.json"
        caminho_arquivo = os.path.join(bronze, nome_arquivo)

        with open(caminho_arquivo, 'w') as arquivo:
            json.dump(dados, arquivo)

        print(f"Dados Tesouro salvo em {caminho_arquivo}")
    else:
        print(f"Erro ao buscar dados do Tesouro: {r.status_code} -{r.text}")

def captura_indicadores_tecnicos(INDICADORES, TICKERS):
    for indicador, ticker in zip(INDICADORES, TICKERS):
        parameters = {
            "function": "TECHNICAL_INDICATOR",
            "symbol": ticker,
            "interval": "daily",
            "time_period": "10",
            "series_type": "close",
            "apikey": API_KEY
        }

        r = requests.get(url, params=parameters)
        if r.status_code == 200:
            dados = r.json()
            nome_arquivo = f"{ticker}_{indicador}.json"
            caminho_arquivo = os.path.join(bronze, nome_arquivo)

            with open(caminho_arquivo, 'w') as arquivo:
                json.dump(dados, arquivo)

            print(f"Dados {indicador} para o ticker {ticker} salvo em {caminho_arquivo}")
        else:
            print(f"Erro ao buscar dados do {indicador} para o ticker {ticker}: {r.status_code} - {r.text}")

fetch_stock_data(TICKERS)
dados_tesouro()
captura_indicadores_tecnicos(INDICADORES, TICKERS)
