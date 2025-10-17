import requests
from airflow.hooks.base import BaseHook

class AlphaVantageHook(BaseHook):
    """
    Hook para acessar a API Alpha Vantage usando uma conexão configurada no Airflow.
    Espera uma Connection ID no formato:
      Conn ID: alpha_vantage_api
      Conn Type: HTTP
      Extra: {"api_key": "SUA_CHAVE_API"}
    """

    def __init__(self, conn_id="alpha_vantage_api"):
        super().__init__()
        self.conn_id = conn_id

    def get_conn_params(self):
        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        api_key = extras.get("api_key")
        base_url = conn.host or "https://www.alphavantage.co/query"
        return base_url, api_key

    def run_query(self, **params):
        base_url, api_key = self.get_conn_params()
        params["apikey"] = api_key
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
