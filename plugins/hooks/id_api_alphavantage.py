from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook


class AlphaVantageHook(HttpHook):
    def __init__(self, conn_id: str = "alpha_vantage_api"):
        # define GET como padrão
        super().__init__(http_conn_id=conn_id, method="GET")
        self.conn_id = conn_id

        conn = BaseHook.get_connection(self.conn_id)
        self.api_key = conn.extra_dejson.get("API_KEY_STOCK")
        if not self.api_key:
            raise ValueError("❌ API_KEY_STOCK não encontrada no Extra da Connection.")

    def run_query(self, **params):
        params["apikey"] = self.api_key

        response = self.run(
            endpoint="query",
            data=params  # ✅ aqui é data, não params
        )

        return response.json()

