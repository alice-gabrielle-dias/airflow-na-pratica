from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

# Função responsável por imprimir números de 1 a 5 no log
def imprimir_numeros_de_1_a_5():
    for i in range(1, 6):
        print(i)
        
# Definição da DAG (fluxo de execução no Airflow)   
with DAG(
    dag_id= "numeros_de_1_a_5", 
    start_date= datetime(2024, 6, 1), # Data a partir da qual a DAG pode ser executada
    schedule=None, # DAG executada manualmente (sem agendamento automático)
    catchup=False # Ignora execuções antigas
) as dag:
    # Task que executa a função de impressão
    tarefa_imprimir_numeros = PythonOperator(
        task_id= "imprimir_numeros_de_1_a_5",
        python_callable=imprimir_numeros_de_1_a_5
    )
    
