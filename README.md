# Objetivo do projeto
1. **Airflow coleta** dados brutos de APIs (bronze) e salvar em nosso datalake no MiniO.
2. **Airflow move** esses dados pro DW (silver).
3. **Airflow dispara dbt** (`dbt run`) para transformar tabelas (gold).
4. **Airflow dispara dbt test** ou validações.
5. **Airflow notifica** (Slack, e-mail) em caso de falha.

# Ferramentas utilizadas e para o que serve cada uma
MinIO é um servidor de armazenamento de objetos (object storage), compatível com o protocolo S3 da AWS. Em termos simples: ele permite guardar e acessar arquivos (como .csv, .json, .parquet, .jpg, .zip, etc.) de forma segura, escalável e acessível via API HTTP. De forma mais objetiva, podemos pensar nele como um “Google Drive profissional”, feito pra sistemas e pipelines, não pra humanos.
Em resumo, O MinIO segue a mesma lógica da AWS S3, onde nele você cria cria buckets (como “pastas” principais), dentro deles, guarda objetos (arquivos) e cada objeto pode ser acessado via endpoint HTTP, com permissões e autenticação. O MinIO é usado como uma opção para substituir o Amazon S3 (funcionando da mesma forma, porém local), permitindo um data lake local ou híbrido (on-premise + cloud).
Estrutura de pastas/arquivos MiniO:
s3://datalake/

```bash
# Estrutura do Data Lake

│
├── bronze/
│   ├── alphavantage/
│   │   ├── function=TIME_SERIES_DAILY/
│   │   │   ├── date=2025-10-17/
│   │   │   │   ├── PETR4.SA.parquet
│   │   │   │   ├── AAPL.parquet
│   │   │   │   └── ...
│   │   └── function=TREASURY_YIELD/
│   │       ├── date=2025-10-17/
│   │       │   ├── tesouro.parquet
│   │       │   └── ...
│
├── silver/
│   ├── alphavantage/
│   │   ├── function=TIME_SERIES_DAILY/
│   │   │   └── clean/
│   │   │       ├── PETR4.parquet
│   │   │       └── VALE3.parquet
│
└── gold/
    ├── indicadores/
    │   ├── rentabilidade_mensal.parquet
    │   └── media_precos.parquet



