"""
DAG: etl_nomes

Pipeline ETL simples utilizando Apache Airflow.

Etapas:
1. Extração: leitura de um arquivo CSV contendo nomes
2. Transformação: limpeza, padronização (maiúsculas) e remoção de duplicados
3. Carga: escrita dos dados tratados em um novo arquivo CSV

Objetivo:
Praticar conceitos de ETL, organização de DAGs e fluxo de dados entre tarefas no Airflow.
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def extrair_dados():
    df = pd.read_csv("/opt/airflow/data/nomes.csv")
    return df["nome"].tolist()

def transformar_dados(nomes):
    nomes_limpos = []

    for nome in nomes:
        nome_limpo = nome.strip().upper()
        nomes_limpos.append(nome_limpo)

    nomes_unicos = list(set(nomes_limpos))

    return nomes_unicos

def carregar_dados(nomes):
    # Cria (ou sobrescreve) o arquivo de saída
    with open("/opt/airflow/output/nomes_tratados.csv", "w") as f:
        f.write("nome\n")
        # percorre a lista de nomes tratados
        for nome in nomes:
            f.write(f"{nome}\n")
            
# Definição da DAG
with DAG(
    dag_id= "etl_nomes",
    start_date= datetime(2024, 6, 1),
    schedule=None, # Execução manual (sem agendamento)
    catchup=False # Não executa DAGs passadas
) as dag:
    
    task_extrair = PythonOperator(
        task_id= "extrair_dados",
        python_callable=extrair_dados
    )
    
    task_transformar = PythonOperator(
        task_id= "transformar_dados",
        python_callable=transformar_dados,
        op_args=[task_extrair.output] # Passa a saída da tarefa de extração como argumento
    )
    
    task_carregar = PythonOperator(
        task_id= "carregar_dados",
        python_callable=carregar_dados,
        op_args=[task_transformar.output] # Passa a saída da tarefa de transformação como argumento
    )
    
    # Define a ordem da execução
    task_extrair >> task_transformar >> task_carregar