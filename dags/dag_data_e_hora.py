from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pytz


def data_e_hora():
    fuso_brasil = pytz.timezone("America/Sao_Paulo")
    now = datetime.now(fuso_brasil).strftime("%d/%m/%Y %H:%M:%S")
    print(f"Data e Hora: {now}")
    
with DAG(
    dag_id= 'data_e_hora_atual',
    start_date= datetime(2024, 6, 1),
    schedule=None,
    catchup=False
) as dag:
    tarefa_data_e_hora = PythonOperator(
        task_id= 'imprimir_data_e_hora',
        python_callable=data_e_hora
    )