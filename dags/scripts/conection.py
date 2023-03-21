import psycopg2
import requests
import base64

# Conexão com o banco de dados
def connect_to_db():
    db_host = "isilo.db.elephantsql.com"
    db_name = "eeurnzyw"
    db_user = "eeurnzyw"
    db_password = "H9V-Kr3rLn4KCSQ4DbjbI179rokws3G8"

    conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_password)

    return conn

# Conexão com API Spotify
def get_access_token(client_id, client_secret):
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_header = base64.b64encode(f'{client_id}:{client_secret}'.encode('utf-8')).decode('ascii')
    headers = {'Authorization': f'Basic {auth_header}'}
    data = {'grant_type': 'client_credentials'}
    response = requests.post(auth_url, headers=headers, data=data)
    access_token = response.json()['access_token']
    
    return access_token