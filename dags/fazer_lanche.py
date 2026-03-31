from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def pegar_pao():
    print("Pegando pão")
    
def colocar_recheio():
    print("Colocando recheio")
    
def montar_lanche():
    print("Montando o lanche")
    
def comer_lanche():
    print("Comendo o lanche")
    
with DAG(
    dag_id= "fazer_lanche",
    start_date= datetime(2024, 6, 1),
    schedule=None, # Execução manual (sem agendamento)
    catchup=False, # Não executa DAGs passadas
) as dag:
    
    task1= PythonOperator(
        task_id= "pegar_pao",
        python_callable=pegar_pao
    )
    
    task2= PythonOperator(
        task_id= "colocar_recheio",
        python_callable=colocar_recheio
    )
    
    task3= PythonOperator(
        task_id= "montar_lanche",
        python_callable=montar_lanche
    )
    
    task4= PythonOperator(
        task_id= "comer_lanche",
        python_callable=comer_lanche
    )
    
    task1 >> task2 >> task3 >> task4