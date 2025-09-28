import os
import json
from hooks.id_api_alphavantage import AlphaVantageHook

bronze = "./bronze"  # ajuste para seu caminho de bronze

TICKERS = [
    "AAPL", "GOOGL", "MSFT",       # Ações estrangeiras
    "PETR4.SA", "VALE3.SA", "ITUB4.SA"  # Ações brasileiras
]

INDICADORES = [
    "EMA", "SMA", "CCI", "WMA", "DEMA",
    "TEMA", "KAMA", "ADX", "RSI",
    "WILLR", "OBV"
]


def fetch_stock_data():
    os.makedirs(bronze, exist_ok=True)
    hook = AlphaVantageHook(conn_id="alpha_vantage_api")

    for ticker in TICKERS:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": ticker,
            "outputsize": "compact",  # ou "full"
            "datatype": "json",
        }

        try:
            dados = hook.run_query(**params)
            caminho_arquivo = os.path.join(bronze, f"{ticker}.json")

            with open(caminho_arquivo, "w") as arquivo:
                json.dump(dados, arquivo)

            print(f"Dados ticker {ticker} salvo em {caminho_arquivo}")

        except Exception as e:
            print(f"Erro ao buscar {ticker}: {e}")


def dados_tesouro():
    os.makedirs(bronze, exist_ok=True)
    hook = AlphaVantageHook(conn_id="alpha_vantage_api")

    params = {
        "function": "TREASURY_YIELD",
        "interval": "daily",
        "maturity": "2year"
    }

    dados = hook.run_query(**params)

    caminho_arquivo = os.path.join(bronze, "tesouro.json")
    with open(caminho_arquivo, "w") as arquivo:
        json.dump(dados, arquivo)

    print(f"Dados Tesouro salvo em {caminho_arquivo}")


def captura_indicadores_tecnicos():
    os.makedirs(bronze, exist_ok=True)
    hook = AlphaVantageHook(conn_id="alpha_vantage_api")

    for indicador, ticker in zip(INDICADORES, TICKERS):
        params = {
            "function": indicador,   # cada indicador (ex: "EMA", "RSI", etc)
            "symbol": ticker,
            "interval": "daily",
            "time_period": "10",
            "series_type": "close"
        }

        try:
            dados = hook.run_query(**params)
            nome_arquivo = f"{ticker}_{indicador}.json"
            caminho_arquivo = os.path.join(bronze, nome_arquivo)

            with open(caminho_arquivo, "w") as arquivo:
                json.dump(dados, arquivo)

            print(f"Dados {indicador} para {ticker} salvo em {caminho_arquivo}")

        except Exception as e:
            print(f"Erro ao buscar {indicador} para {ticker}: {e}")