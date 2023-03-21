import base64
import requests
import os
import pandas as pd

def table_5(access_token):
    # GET para a API do Spotify com o termo de pesquisa
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'q': 'data hackers',
        'type': 'show',
        'market': 'BR',
        'limit': 50
    }
    response = requests.get('https://api.spotify.com/v1/search', headers=headers, params=params)
    data = response.json()

    # Extraindo podcasts encontrados e adicionando em podcasts
    podcasts = []
    for item in data['shows']['items']:
        podcast = {
            'name': item['name'],
            'description': item['description'],
            'id': item['id'],
            'total_episodes': item['total_episodes']
        }
        podcasts.append(podcast)

    # Crie a tabela 5 com os dados extraídos e salva no csv
    df_table_5 = pd.DataFrame(podcasts, columns=['name', 'description', 'id', 'total_episodes'])

    podcast_table_csv("table_5", df_table_5)

def table_6(access_token):
    table_5 = pd.read_csv('/opt/bitnami/airflow/dags/data_target/podcast/table_5.csv')
    # Encontra o ID do podcast Data Hackers na tabela 5
    data_hackers_podcast = table_5[table_5['name'] == 'Data Hackers']
    data_hackers_id = data_hackers_podcast['id'].values[0]
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'q': 'data hackers',
        'type': 'show',
        'market': 'BR',
        'limit': 50
    }

    # Faz o get na API com o id passado
    response = requests.get(f'https://api.spotify.com/v1/shows/{data_hackers_id}/episodes', headers=headers, params=params)
    data = response.json()
    episodes = []
    for item in data['items']:
        episode = {
            'id': item['id'],
            'name': item['name'],
            'description': item['description'],
            'release_date': item['release_date'],
            'duration_ms': item['duration_ms'],
            'language': item['language'],
            'explicit': item['explicit'],
            'type': item['type']
        }
        episodes.append(episode)

    df_table_6 = pd.DataFrame(episodes, columns=['id', 'name', 'description', 'release_date', 'duration_ms', 'language', 'explicit', 'type'])

    podcast_table_csv("table_6", df_table_6)


def table_7():
    # Filtro de episódios com participação do Grupo Boticário
    table_6 = pd.read_csv('/opt/bitnami/airflow/dags/data_target/podcast/table_6.csv')

    df_table_7 = table_6[table_6['description'].str.contains('Boticário')]

    podcast_table_csv("table_7", df_table_7)

def podcast_table_csv(table_name, df):

    # Salvando os dados em CSV
    target_folder = "/opt/bitnami/airflow/dags/data_target/podcast"

    # Verificando se a pasta já existe, se não existir, ela será criada
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
        
    target_file = f"{table_name}.csv"
    full_path = os.path.join(target_folder, target_file)
    df.to_csv(full_path, index=False)

    print(f"Tabela {table_name} salva em formato CSV no caminho {full_path}")