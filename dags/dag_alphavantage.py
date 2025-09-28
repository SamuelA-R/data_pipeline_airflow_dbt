from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# importa funções da camada de coleta
from tasks.coleta import dados_tesouro, fetch_stock_data, captura_indicadores_tecnicos

with DAG(
    dag_id="dag_alphavantage",
    start_date=days_ago(1),
    schedule_interval="@daily",  # roda uma vez por dia
    catchup=False,
    tags=["alpha_vantage", "mercado", "tesouro"],
) as dag:

    coleta_tesouro = PythonOperator(
        task_id="coleta_tesouro",
        python_callable=dados_tesouro,
    )

    coleta_tickers = PythonOperator(
        task_id="coleta_tickers",
        python_callable=fetch_stock_data,
    )

    coleta_indicadores = PythonOperator(
        task_id="coleta_indicadores",
        python_callable=captura_indicadores_tecnicos,  # ✅ agora sem args
    )

    # fluxo: tesouro -> tickers -> indicadores
    coleta_tesouro >> coleta_tickers >> coleta_indicadores
