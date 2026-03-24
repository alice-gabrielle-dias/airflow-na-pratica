from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def imprimir_numeros_pares():
    for i in range(2, 11, 2):
        print(i)
            
with DAG(
    dag_id= "numeros_pares",
    start_date= datetime(2024, 6, 1),
    schedule=None, # Execução manual (sem agendamento)
    catchup=False, # Não executa DAGs passadas
) as dag:
    
    tarefa_numeros_pares = PythonOperator(
        task_id= "numeros_pares",
        python_callable=imprimir_numeros_pares
    )