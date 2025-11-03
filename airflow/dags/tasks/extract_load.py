import os
import json
from hooks.id_api_alphavantage import AlphaVantageHook
from sqlalchemy import create_engine
from datetime import datetime
import logging
import pandas as pd

salvar_dados = "/opt/airflow/temp_data"
date_today = datetime.now().strftime("%Y-%m-%d")


def captura_ativos(TICKERS):
    os.makedirs(salvar_dados, exist_ok=True)
    hook = AlphaVantageHook(conn_id="alpha_vantage_api")
    for ticker in TICKERS:
        params = {"function": "TIME_SERIES_DAILY", "symbol": ticker, "outputsize": "compact", "datatype": "json"}
        dados = hook.run_query(**params)
        caminho = os.path.join(salvar_dados, f"acao_{ticker}-{date_today}.json")
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
            caminho = os.path.join(salvar_dados, f"Indicador_{ticker}_{indicador}-{date_today}.json")
            with open(caminho, "w") as f:
                json.dump(dados, f)
    
    return "Indicadores técnicos salvos"


def limpar_pasta_temporaria():
    if os.path.exists(salvar_dados):
        for arquivo in os.listdir(salvar_dados):
            caminho_arquivo = os.path.join(salvar_dados, arquivo)
            if os.path.isfile(caminho_arquivo):
                os.remove(caminho_arquivo)
        return "Arquivos da pasta temporária excluídos"
    else:
        return "Pasta temporária não existe"


def carrega_ativos_para_postgres():
    arquivos_json = [f for f in os.listdir(CAMINHO_BRONZE) if f.startswith("acao_") and f.endswith(".json")]
    dataframes = []

    for arquivo in arquivos_json:
        caminho_arquivo = os.path.join(CAMINHO_BRONZE, arquivo)
        with open(caminho_arquivo, 'r') as f:
            dados = json.load(f)
            if "Time Series (Daily)" in dados:
                df = pd.DataFrame.from_dict(dados["Time Series (Daily)"], orient="index")
                df.reset_index(inplace=True)
                df.rename(columns={
                    "index": "data",
                    "1. open": "open",
                    "2. high": "high",
                    "3. low": "low",
                    "4. close": "close",
                    "5. volume": "volume"
                }, inplace=True)
                df["ticker"] = dados["Meta Data"]["2. Symbol"]
                df = df.astype({
                    "data": "datetime64",
                    "open": "float",
                    "high": "float",
                    "low": "float",
                    "close": "float",
                    "volume": "int64"
                })
                dataframes.append(df)

    if dataframes:
        df_final = pd.concat(dataframes, ignore_index=True)
        engine = create_engine('postgresql://postgres:postgres@postgres:5434/postgres-dw')
        with engine.connect() as conn:
            df_final.to_sql('ativos', con=conn, schema='bronze', if_exists='replace', index=False)


def carrega_tesouro_para_postgres():
    arquivos_json = [f for f in os.listdir(salvar_dados) if f.startswith("tesouro") and f.endswith(".json")]
    dataframes = []

    for arquivo in arquivos_json:
        caminho_arquivo = os.path.join(salvar_dados, arquivo)
        with open(caminho_arquivo, 'r') as f:
            dados = json.load(f)
            if "data" in dados:
                df = pd.DataFrame(dados["data"])
                df.rename(columns={
                    "date": "data",
                    "value": "valor"
                }, inplace=True)
                df['valor'] = pd.to_numeric(df['valor'], errors='coerce').round(3)
                df = df.astype({
                    "data": "datetime64",
                    "valor": "float"
                })
                dataframes.append(df)

    logging.debug(f"O arquivo deu certo, tem dados {print(dataframes)}")
    if dataframes:
        df_final = pd.concat(dataframes, ignore_index=True)
        logging.warning(f"O arquivo deu certo, tem dados {df_final.head()}")
        engine = create_engine('postgresql://postgres:postgres@postgres:5434/postgres-dw')
        with engine.connect() as conn:
            df_final.to_sql('tesouro', con=conn, schema='bronze', if_exists='replace', index=False)

def carrega_indicadores_para_postgres():
    arquivos_json = [f for f in os.listdir(salvar_dados) if f.startswith("indicador_") and f.endswith(".json")]
    dataframes = []

    for arquivo in arquivos_json:
        caminho_arquivo = os.path.join(salvar_dados, arquivo)
        with open(caminho_arquivo, 'r') as f:
            dados = json.load(f)
            for key in dados:
                if key.startswith("Technical Analysis"):
                    df = pd.DataFrame.from_dict(dados[key], orient="index")
                    df.reset_index(inplace=True)
                    df.rename(columns={
                        "index": "data",
                        list(df.columns)[1]: "valor"
                    }, inplace=True)
                    df["ticker"] = dados["Meta Data"]["1: Symbol"]
                    df["indicador"] = dados["Meta Data"]["2: Indicator"]
                    df = df.astype({
                        "data": "datetime64",
                        "valor": "float"
                    })
                    dataframes.append(df)

    if dataframes:
        df_final = pd.concat(dataframes, ignore_index=True)
        engine = create_engine('postgresql://postgres:postgres@postgres:5434/postgres-dw')
        with engine.connect() as conn:
            df_final.to_sql('indicadores', con=conn, schema='bronze', if_exists='replace', index=False)