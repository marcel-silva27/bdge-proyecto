from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

# Configuración de resiliencia
default_args = {
    'owner': 'marce',
    'retries': 3,                 # Reintentar 3 veces si falla
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False
}

with DAG(
    'global_mart_consolidation_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 6, 26),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Ejecutar el pipeline de Beam
    extract_and_transform_silver = BashOperator(
        task_id='extract_and_transform_silver',
        bash_command='python /opt/airflow/beam/pipeline.py --sales_path /opt/airflow/data/raw/sales_data.csv --logs_path /opt/airflow/data/raw/status_logs.csv'
    )

    # Task 2: Sensor para verificar el archivo antes de que dbt intente procesar
    sensor_silver_data = FileSensor(
        task_id='sensor_silver_data',
        filepath='/opt/airflow/data/silver/sales_enriched.parquet',
        fs_conn_id='fs_default', # Usa la conexión local por defecto
        poke_interval=10,        # Revisa cada 10 segundos
        timeout=120,               # Falla si después de 120 segundos no está
        mode='poke'               # Modo de espera activa
    )

    # Task 3: Load y Modelado con dbt
    load_and_model_gold = BashOperator(
        task_id='load_and_model_gold',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir . && dbt test --profiles-dir .',
    )

    # Definición de dependencias y flujo
    extract_and_transform_silver >> sensor_silver_data >> load_and_model_gold