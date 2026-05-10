import os
import json
from datetime import datetime
from time import sleep
import boto3
from tmdbv3api import TMDb, Movie, Discover

# Configuração TMDB
tmdb = TMDb()
tmdb.api_key = os.getenv("API_KEY")
movie_api = Movie()
discover = Discover()

# Configuração AWS
s3 = boto3.client('s3')
bucket = 'datalake-gabrielatrombetta'

# Parâmetros
batch_size = 100 # filmes por arquivo
max_pages = 10 # páginas da API

# Busca filmes de um gênero, organiza em batches e envia para o S3
def processar_genero(id_genero, nome_genero):
    data = datetime.now()
    arquivos = 1
    filmes = []
    path = f"{data.year}/{data.month:02}/{data.day:02}"

    for page in range(1, max_pages+1):
        for filme in discover.discover_movies({'with_genres': id_genero, 'sort_by': 'popularity.desc', 'page': page}):
            try:
                movie = movie_api.details(filme.id)

                # keywords
                keywords_obj = movie_api.keywords(filme.id)
                keywords_list = [kw['name'] for kw in keywords_obj['keywords']] if hasattr(keywords_obj, 'keywords') else []
                
                # elenco
                credits = movie_api.credits(filme.id)
                cast_list = []
                if hasattr(credits, 'cast'):
                    for c in credits.cast:
                        cast_list.append({
                            "name": c.name,
                            "character": getattr(c, "character", None),
                            "gender": getattr(c, "gender", None)
                        })

                # adiciona filme à lista
                filmes.append({
                    "id": movie.id,
                    "title": movie.title,
                    "release_date": movie.release_date,
                    "popularity": movie.popularity,
                    "vote_average": movie.vote_average,
                    "vote_count": movie.vote_count,
                    "genre_main": nome_genero,
                    "genres": [g["name"] for g in getattr(movie, "genres", [])],
                    "keywords": keywords_list,
                    "cast": cast_list
                })

                # envia o lote para o S3 quando atinge o tamanho definido
                if len(filmes) >= batch_size:
                    nome = f'Raw/TMDB/JSON/{nome_genero}/{path}/{nome_genero.lower()}_{arquivos:03d}.json'
                    s3.put_object(Bucket=bucket, Key=nome, Body=json.dumps(filmes, ensure_ascii=False).encode('utf-8'))
                    print(f"Enviado: s3://{bucket}/{nome}")
                    arquivos += 1
                    filmes = []
                sleep(0.1)
            except Exception as e:
                print(f"Erro ao processar filme: {e}")

    # envia o restante (úlimo lote)
    if filmes:
        nome = f'Raw/TMDB/JSON/{nome_genero}/{path}/{nome_genero.lower()}_{arquivos:03d}.json'
        s3.put_object(Bucket=bucket, Key=nome, Body=json.dumps(filmes, ensure_ascii=False).encode('utf-8'))
        print(f"Enviado: s3://{bucket}/{nome}")

def lambda_handler(event, context):
    genero = event.get("tipo")
    ids = {"Romance": 10749, "Drama": 18}
    if genero in ids:
        processar_genero(ids[genero], genero)
    return {"status":"ok","mensagem":f"Processamento '{genero}' concluído"}
