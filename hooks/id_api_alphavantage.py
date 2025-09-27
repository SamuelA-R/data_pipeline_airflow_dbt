from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook

class AlphaVantageHook(HttpHook):
    """
    Hook para Alpha Vantage.
    Pega a API_KEY da Connection configurada no Airflow
    e injeta automaticamente em todas as requisições.
    """

    def __init__(self, conn_id: str = "alpha_vantage_api"):
        super().__init__(http_conn_id=conn_id)
        self.conn_id = conn_id
        self.base_url = "https://www.alphavantage.co/query"

        # pega a api_key direto do Extra
        conn = BaseHook.get_connection(self.conn_id)
        self.api_key = conn.extra_dejson.get("API_KEY_STOCK")

    def run_query(self, **params):
        """Executa a requisição com parâmetros + api_key injetada"""
        params["apikey"] = self.api_key  # injeta a key do Airflow
        session = self.get_conn()
        response = session.get(self.base_url, params=params)
        response.raise_for_status()
        return response.json()