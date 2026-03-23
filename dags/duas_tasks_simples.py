from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

"""
DAG de exemplo com duas tasks simples.

Esta DAG demonstra:
- Definição de duas tasks com PythonOperator
- Task 1 executa antes da Task 2 (dependência)
- Execução manual (schedule=None)
- Sem execução retroativa (catchup=False)
"""

# Função da primeira task
def tarefa_um():
    print("Executando tarefa um")
    
# Função da segunda task
def tarefa_dois():
    print("Executando tarefa dois")
    
# Definição da DAG 
with DAG(
    dag_id= "duas_tasks_simples",
    start_date=datetime(2024, 6, 1),
    schedule=None, # Execução manual (sem agendamento)
    catchup=False
) as dag:
    
    # Task 1
    task_um = PythonOperator(
        task_id= "tarefa_um",
        python_callable=tarefa_um
    ) 
    
    # Task 2
    task_dois = PythonOperator(
        task_id= "tarefa_dois",
        python_callable=tarefa_dois
    ) 
    
    # Definindo dependência: task_1 executa antes de task_2
    task_um >> task_dois
    
    
    
    