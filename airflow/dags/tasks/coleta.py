import os
import json
from hooks.id_api_alphavantage import AlphaVantageHook
from datetime import datetime

bronze = "/opt/airflow/data/bronze"

TICKERS = ["AAPL", "GOOGL", "MSFT", "PETR4.SA", "VALE3.SA", "ITUB4.SA"]
INDICADORES = ["EMA","SMA","CCI","WMA","DEMA","TEMA","KAMA","ADX","RSI","WILLR","OBV"]
date_today = datetime.now().strftime("%Y-%m-%d")

def fetch_stock_data():
    os.makedirs(bronze, exist_ok=True)
    hook = AlphaVantageHook(conn_id="alpha_vantage_api")
    for ticker in TICKERS:
        params = {"function": "TIME_SERIES_DAILY", "symbol": ticker, "outputsize": "compact", "datatype": "json"}
        dados = hook.run_query(**params)
        caminho = os.path.join(bronze, f"{ticker}-{date_today}.json")
        with open(caminho, "w") as f:
            json.dump(dados, f)
    return f"{len(TICKERS)} tickers salvos"

def dados_tesouro():
    os.makedirs(bronze, exist_ok=True)
    hook = AlphaVantageHook(conn_id="alpha_vantage_api")
    params = {"function": "TREASURY_YIELD", "interval": "daily", "maturity": "2year"}
    dados = hook.run_query(**params)
    caminho = os.path.join(bronze, "tesouro-{date_today}.json")
    with open(caminho, "w") as f:
        json.dump(dados, f)
    return "Tesouro salvo"

def captura_indicadores_tecnicos():
    os.makedirs(bronze, exist_ok=True)
    hook = AlphaVantageHook(conn_id="alpha_vantage_api")
    for indicador, ticker in zip(INDICADORES, TICKERS):
        params = {"function": indicador, "symbol": ticker, "interval": "daily", "time_period": "10", "series_type": "close"}
        dados = hook.run_query(**params)
        caminho = os.path.join(bronze, f"{ticker}_{indicador}-{date_today}.json")
        with open(caminho, "w") as f:
            json.dump(dados, f)
    return "Indicadores técnicos salvos"

