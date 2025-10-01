from airflow.decorators import dag, task
from datetime import datetime, timedelta
from tasks.coleta import dados_tesouro, fetch_stock_data, captura_indicadores_tecnicos

default_args = {"retries": 3, "retry_delay": timedelta(minutes=5)}

@dag(
    dag_id="dag_alphavantage",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["alpha_vantage", "mercado", "tesouro"],
)
def dag_alphavantage():
    @task
    def coleta_tesouro():
        return dados_tesouro()

    @task
    def coleta_tickers():
        return fetch_stock_data()

    @task
    def coleta_indicadores():
        return captura_indicadores_tecnicos()

    coleta_tesouro() >> coleta_tickers() >> coleta_indicadores()

dag_instance = dag_alphavantage()
