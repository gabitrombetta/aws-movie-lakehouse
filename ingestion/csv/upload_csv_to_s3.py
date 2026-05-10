import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

bucket_name = 'datalake-gabrielatrombetta'

data = datetime.now()
path = f'{data.year}/{data.month:02}/{data.day:02}'

movies = 'movies.csv'
series = 'series.csv'

path_filmes = f'Raw/Local/CSV/Movies/{path}/{movies}'
path_series = f'Raw/Local/CSV/Series/{path}/{series}'

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
    region_name='us-east-1'
)
print('Conexão com AWS realizada')

s3_client.upload_file('data/movies.csv', bucket_name, path_filmes)
print(f'Filmes enviado para s3://{bucket_name}/{path_filmes}')


s3_client.upload_file('data/series.csv', bucket_name, path_series)
print(f'Series enviado para s3://{bucket_name}/{path_series}')
