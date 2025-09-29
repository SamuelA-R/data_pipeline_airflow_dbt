# Objetivo do projeto
1. **Airflow coleta** dados brutos de APIs (bronze).
2. **Airflow move** esses dados pro DW (silver).
3. **Airflow dispara dbt** (`dbt run`) para transformar tabelas (gold).
4. **Airflow dispara dbt test** ou validações.
5. **Airflow notifica** (Slack, e-mail) em caso de falha.

