from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

# Função responsável por realizar a soma e exibir o resultado no log do Airflow
def somar_dois_numeros():
    resultado = 2 + 5
    print(f"O resultado da soma é: {resultado}")

# Definição da DAG (fluxo de execução no Airflow)
with DAG(
    dag_id= "somar_dois_numeros",
    start_date= datetime(2024, 6, 1),
    schedule=None, # Execução manual (sem agendamento)
    catchup=False, # Não executa DAGs passadas
) as dag:
    # Task que executa a função de soma
    tarefa_somar_dois_numeros = PythonOperator(
        task_id= "somar_dois_numeros",
        python_callable=somar_dois_numeros
    )