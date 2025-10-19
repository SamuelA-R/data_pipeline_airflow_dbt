from airflow.decorators import dag, task
from datetime import datetime, timedelta
from tasks.coleta import dados_tesouro, fetch_stock_data, captura_indicadores_tecnicos
from tasks.connect_to_minIO import MinIOConnect

date_today = datetime.now().strftime("%Y-%m-%d")
TICKERS = ["AAPL", "GOOGL", "MSFT", "PETR4.SA", "VALE3.SA", "ITUB4.SA"]
INDICADORES = ["EMA","SMA","CCI","WMA","DEMA","TEMA","KAMA","ADX","RSI","WILLR","OBV"]

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
        return fetch_stock_data(TICKERS=TICKERS)

    @task
    def coleta_indicadores():
        return captura_indicadores_tecnicos(INDICADORES=INDICADORES, TICKERS=TICKERS)
    
    @task
    def upload_to_minio_tickers():
        upload_tickers = MinIOConnect.connect_and_upload_files_to_minio(
            bucket_name="bronze",
            list_file_name=TICKERS,
            base_path="/opt/airflow/tem_data",
            object_name=None,
            date_today=date_today
        )
        return upload_tickers
    
    @task
    def upload_to_minio_indicadores():
        upload_indicadores = MinIOConnect.connect_and_upload_files_to_minio(
            bucket_name="bronze",
            list_file_name=INDICADORES,
            base_path="/opt/airflow/tem_data",
            object_name=None,
            date_today=date_today
        )
        return upload_indicadores
    
    @task
    def upload_to_minio_tesouro():
        upload_tesouro = MinIOConnect.connect_and_upload_files_to_minio(
            bucket_name="bronze",
            list_file_name=["tesouro-{date_today}.json"],
            base_path="/opt/airflow/tem_data",
            object_name=None,
            date_today=date_today
        )
        return upload_tesouro

    coleta_tesouro() >> coleta_tickers() >> coleta_indicadores() 


dag_instance = dag_alphavantage()
