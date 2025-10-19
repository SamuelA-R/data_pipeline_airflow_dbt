import os
import json
from hooks.id_api_alphavantage import AlphaVantageHook
from datetime import datetime

salvar_dados = "/opt/airflow/temp_data"
date_today = datetime.now().strftime("%Y-%m-%d")

def fetch_stock_data(TICKERS):
    os.makedirs(salvar_dados, exist_ok=True)
    hook = AlphaVantageHook(conn_id="alpha_vantage_api")
    for ticker in TICKERS:
        params = {"function": "TIME_SERIES_DAILY", "symbol": ticker, "outputsize": "compact", "datatype": "json"}
        dados = hook.run_query(**params)
        caminho = os.path.join(salvar_dados, f"{ticker}-{date_today}.json")
        with open(caminho, "w") as f:
            json.dump(dados, f)
    return f"{len(TICKERS)} tickers salvos"

def dados_tesouro():
    os.makedirs(salvar_dados, exist_ok=True)  # ← corrigido
    hook = AlphaVantageHook(conn_id="alpha_vantage_api")
    params = {"function": "TREASURY_YIELD", "interval": "daily", "maturity": "2year"}
    dados = hook.run_query(**params)
    caminho = os.path.join(salvar_dados, f"tesouro_{date_today}.json")  # ← corrigido
    with open(caminho, "w") as f:
        json.dump(dados, f)
    return "Tesouro salvo"

def captura_indicadores_tecnicos(TICKERS, INDICADORES):
    os.makedirs(salvar_dados, exist_ok=True)
    hook = AlphaVantageHook(conn_id="alpha_vantage_api")
    
    for ticker in TICKERS:
        for indicador in INDICADORES:
            params = {
                "function": indicador,
                "symbol": ticker,
                "interval": "daily",
                "time_period": "10",
                "series_type": "close"
            }
            dados = hook.run_query(**params)
            caminho = os.path.join(salvar_dados, f"{ticker}_{indicador}-{date_today}.json")
            with open(caminho, "w") as f:
                json.dump(dados, f)
    
    return "Indicadores técnicos salvos"



