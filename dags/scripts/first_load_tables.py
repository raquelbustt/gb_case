import csv
import os
from pathlib import Path
from scripts.conection import connect_to_db

def first_load_tables():
    # Definindo o caminho dos arquivos e variáveis
    current_dir = '/opt/bitnami/airflow/dags/data'
    csv_paths = [f'{current_dir}/Base-{year}.csv' for year in range(2017, 2020)]
    columns = "ID_MARCA INTEGER, MARCA VARCHAR(255), ID_LINHA INTEGER, LINHA VARCHAR(255), DATA_VENDA DATE, QTD_VENDA INTEGER"

    # Query para inserir os dados na tabela
    insert_query = "INSERT INTO {} (ID_MARCA, MARCA, ID_LINHA, LINHA, DATA_VENDA, QTD_VENDA) VALUES (%s, %s, %s, %s, %s, %s)"

    # Conexão com o banco de dados e criação de um objeto para executar os comandos SQL
    conn = connect_to_db()
    cursor = conn.cursor()

    # Criando as tabelas e populando com dados
    for year, csv_path in zip(range(2017,2020), csv_paths):
        # Criando tabelas se elas não existirem
        table_name = f'base_{year}'
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        cursor.execute(create_table_query)
        print(f"Table {table_name} created - done!")

        # Limpando a tabela se ela já existir
        cursor.execute(f"TRUNCATE TABLE {table_name}")

        # Populando as tabelas
        with open(csv_path) as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)  # Ignora a linha de cabeçalho
            for row in csvreader:
                data = (int(row[0]), row[1], int(row[2]), row[3], row[4], int(row[5]))
                cursor.execute(insert_query.format(table_name), data)

            print(f"Table {table_name} data loaded - done!")

    # Finaliza a transação e fecha a conexão
    conn.commit()
    conn.close()
