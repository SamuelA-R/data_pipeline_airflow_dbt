from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
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
    #Grupo de coleta de dados (Tesouro, Tickers, Indicadores)
    with TaskGroup("group_coleta_dados", tooltip="Coleta dados de Tesouro e Ações") as group_coleta_dados:
        
        @task
        def coleta_tesouro():
            return dados_tesouro()

        @task
        def coleta_tickers():
            return fetch_stock_data(TICKERS=TICKERS)

        @task
        def coleta_indicadores():
            return captura_indicadores_tecnicos(INDICADORES=INDICADORES, TICKERS=TICKERS)

        coleta_tesouro() >> coleta_tickers() >> coleta_indicadores()

    #Grupo de upload dos dados para o MinIO
    with TaskGroup("group_upload_minio", tooltip="Upload dos arquivos para o bucket bronze") as group_upload_minio:

        @task
        def upload_dados():
            minio = MinIOConnect(
                endpoint_url="http://minio:9000",
                access_key="minioadmin",
                secret_key="minio@1234!"
            )
            return minio.connect_and_upload_files_to_minio(
                bucket_name="bronze",
                base_path="/opt/airflow/temp_data"
            )

        upload_dados()

    #Dependência entre os grupos
    group_coleta_dados >> group_upload_minio


dag_instance = dag_alphavantage()
