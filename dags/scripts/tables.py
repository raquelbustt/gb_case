from scripts.conection import connect_to_db
import pandas as pd
import os

def create_and_populate_table(table_name, column_names_create, query):
    create_table = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(column_names_create)});'
    with connect_to_db() as conn:
        cursor = conn.cursor()
        cursor.execute(create_table)
        print(f"Table {table_name} created - done!")
        # Deleta a tabela se ela já existir
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        cursor.execute(query)
        conn.commit()
        print(f"Table {table_name} data loaded - done!")

def vendas_ano_mes():
    table_name = 'vendas_ano_mes'
    column_names = ["ano_mes", "qtd_venda_total"]
    column_names_create = ['ano_mes VARCHAR', 'qtd_venda_total INTEGER']

    query = f'''
        INSERT INTO {table_name} ({', '.join(column_names)})
        SELECT DISTINCT
            TO_CHAR(DATE_TRUNC('month', data_venda), 'YYYY-MM') as ano_mes,
            SUM(qtd_venda) as qtd_venda_total
        FROM
            (
                SELECT data_venda, qtd_venda FROM base_2017
                UNION ALL
                SELECT data_venda, qtd_venda FROM base_2018
                UNION ALL
                SELECT data_venda, qtd_venda FROM base_2019
            ) t
        GROUP BY ano_mes
        ORDER BY ano_mes DESC
    '''
    create_and_populate_table(table_name, column_names_create, query)

def vendas_marca_linha():
    table_name = 'vendas_marca_linha'
    column_names = ['marca', 'linha', 'qtd_venda']
    column_names_create = ['marca VARCHAR(255)', 'linha VARCHAR(255)', 'qtd_venda INTEGER']
    query = f'''
        INSERT INTO {table_name} ({', '.join(column_names)})
        SELECT DISTINCT
            marca,
            linha,
            SUM(qtd_venda) as qtd_venda_total
        FROM
            (
                SELECT marca, linha, qtd_venda FROM base_2017
                UNION ALL
                SELECT marca, linha, qtd_venda FROM base_2018
                UNION ALL
                SELECT marca, linha, qtd_venda FROM base_2019
            ) t
        GROUP BY marca, linha
        ORDER BY marca DESC
    '''
    create_and_populate_table(table_name, column_names_create, query)
    print(f"Tabela {table_name} salva no banco de dados!")

def vendas_marca_ano_mes():
    table_name = 'vendas_marca_ano_mes'
    column_names = ['marca', 'ano_mes', 'qtd_venda_total']
    column_names_create = ['marca VARCHAR(255)', 'ano_mes VARCHAR', 'qtd_venda_total INTEGER']
    query = f'''
        INSERT INTO {table_name} ({', '.join(column_names)})
        SELECT DISTINCT
            marca,
            TO_CHAR(DATE_TRUNC('month', data_venda), 'YYYY-MM') as ano_mes,
            SUM(qtd_venda) as qtd_venda_total
        FROM
            (
                SELECT marca, data_venda, qtd_venda FROM base_2017
                UNION ALL
                SELECT marca, data_venda, qtd_venda FROM base_2018
                UNION ALL
                SELECT marca, data_venda, qtd_venda FROM base_2019
            ) t
        GROUP BY marca, ano_mes
        ORDER BY marca DESC
    '''
    create_and_populate_table(table_name, column_names_create, query)
    print(f"Tabela {table_name} salva no banco de dados!")

def vendas_linha_ano_mes():
    # Definindo o nome da tabela e das colunas
    table_name = 'vendas_linha_ano_mes'
    column_names = ['linha', 'ano_mes', 'qtd_venda_total']
    column_names_create = ['linha VARCHAR(255)', 'ano_mes VARCHAR', 'qtd_venda_total INTEGER']

    query = f'''
        INSERT INTO {table_name} ({', '.join(column_names)})
        SELECT DISTINCT
            linha,
            TO_CHAR(DATE_TRUNC('month', data_venda), 'YYYY-MM') as ano_mes,
            SUM(qtd_venda) as qtd_venda_total
        FROM
            (
                SELECT linha, data_venda, qtd_venda FROM base_2017
                UNION ALL
                SELECT linha, data_venda, qtd_venda FROM base_2018
                UNION ALL
                SELECT linha, data_venda, qtd_venda FROM base_2019
            ) t
        GROUP BY linha, ano_mes
        ORDER BY linha DESC
    '''

    create_and_populate_table(table_name, column_names_create, query)
    print(f"Tabela {table_name} salva no banco de dados!")

def read_and_save_data_to_csv(table_name):
    # Conectando ao banco
    conn = connect_to_db()
    
    # Consulta da tabela
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)

    target_folder = "/opt/bitnami/airflow/dags/data_target/vendas"

    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
        
    target_file = f"{table_name}.csv"
    full_path = os.path.join(target_folder, target_file)
    df.to_csv(full_path, index=False)

    print(f"Tabela {table_name} salva em formato CSV no caminho {full_path}!")