from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import statistics

def calcular_media():
    numeros = [10,20,30,40,50]
    media = statistics.mean(numeros)
    print("Média:", media)
    
with DAG(
    dag_id= 'calcular_media',
    start_date= datetime(2024, 6, 1), # Data a partir da qual a DAG pode ser executada
    schedule=None, # DAG executada manualmente (sem agendamento automático)
    catchup=False # # Ignora execuções antigas
) as dag:
    # Task que calcula a média da lista de números
    tarefa_calcular_media = PythonOperator(
        task_id= 'calcular_media',
        python_callable=calcular_media
    )
    
