from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pytz

# Função responsável por obter e imprimir a data e hora atual no fuso do Brasil
def data_e_hora():
    fuso_brasil = pytz.timezone("America/Sao_Paulo") # Define o fuso horário de São Paulo
    now = datetime.now(fuso_brasil).strftime("%d/%m/%Y %H:%M:%S") # Obtém a data e hora atual formatada
    print(f"Data e Hora: {now}")

# Definição da DAG (fluxo de execução no Airflow)    
with DAG(
    dag_id= 'data_e_hora_atual',
    start_date= datetime(2024, 6, 1), # Data a partir da qual a DAG pode ser executada
    schedule=None, # DAG executada manualmente (sem agendamento automático)
    catchup=False # # Ignora execuções antigas
) as dag:
    # Task que executa a função de data e hora
    tarefa_data_e_hora = PythonOperator(
        task_id= 'imprimir_data_e_hora',
        python_callable=data_e_hora
    )