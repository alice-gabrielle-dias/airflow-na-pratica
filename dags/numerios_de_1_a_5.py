from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def imprimir_numeros_de_1_a_5():
    for i in range(1, 6):
        print(i)
     
with DAG(
    dag_id= "numeros_de_1_a_5",
    start_date= datetime(2024, 6, 1),
    schedule=None,
    catchup=False
) as dag:
    tarefa_imprimir_numeros = PythonOperator(
        task_id= "imprimir_numeros_de_1_a_5",
        python_callable=imprimir_numeros_de_1_a_5
    )
    
