import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_LOCAL_PATH', 'S3_TMDB_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df_local = spark.read.parquet(args['S3_LOCAL_PATH'])
df_tmdb = spark.read.parquet(args['S3_TMDB_PATH'])

# Selecionar apenas filmes de Romance e Drama
df_local = df_local.filter((df_local.genero == 'Drama') | (df_local.genero == 'Romance'))

# Converter colunas
df_local = (
    df_local
    .withColumn("anoLancamento", F.col("anoLancamento").cast(IntegerType()))
    .withColumn("tempoMinutos", F.col("tempoMinutos").cast(IntegerType()))
    .withColumn("notaMedia", F.col("notaMedia").cast(FloatType()))
    .withColumn("numeroVotos", F.col("numeroVotos").cast(IntegerType()))
    .withColumn("anoNascimento", F.col("anoNascimento").cast(IntegerType()))
    .withColumn("anoFalecimento", F.col("anoFalecimento").cast(IntegerType()))
)

# Dimensão Filmes
dim_filmes_local = (
    df_local.select(
        F.col("id").cast("string").alias("id_filme"),
        F.col("tituloPincipal").alias("titulo_filme"),
        F.lit("sem registro").alias("keywords_filme"),
        F.col("genero").alias("genero_filme")
    )
)

dim_filmes_tmdb = (
    df_tmdb.withColumn("keyword", F.explode_outer("keywords"))
           .select(
               F.col("id").cast("string").alias("id_filme"),
               F.col("title").alias("titulo_filme"),
               F.col("keyword").alias("keywords_filme"),
               F.col("genre_main").alias("genero_filme")
           )
)

dim_filmes = (
    dim_filmes_local.unionByName(dim_filmes_tmdb, allowMissingColumns=True)
    .fillna("sem registro", subset=["keywords_filme", "genero_filme"])
    .dropDuplicates(["id_filme"])
)

# Dimensão Data
dim_data_local = (
    df_local
    .select(F.col("anoLancamento").alias("ano"))
    .dropna()
)

dim_data_tmdb = (
    df_tmdb
    .select(F.year("release_date").alias("ano"))
    .dropna()
)

dim_data = (
    dim_data_local.union(dim_data_tmdb)
    .dropDuplicates()
    .withColumn("id_data", F.monotonically_increasing_id())
)

# Dimensão Elenco
dim_elenco_local = (
    df_local
    .select(
        F.col("id").cast("string").alias("id_filme"),
        F.col("nomeArtista").alias("nome_artista"),
        F.col("generoArtista").alias("sexo")
    )
    .dropna(subset=["id_filme", "nome_artista"])
)

dim_elenco_tmdb = (
    df_tmdb.withColumn("cast_item", F.explode_outer("cast"))
           .select(
               F.col("id").cast("string").alias("id_filme"),
               F.col("cast_item.name").alias("nome_artista"),
               F.when(F.col("cast_item.gender") == 1, "actress")
                .when(F.col("cast_item.gender") == 2, "actor")
                .otherwise("sem registro").alias("sexo")
           )
           .dropna(subset=["id_filme", "nome_artista"])
)

dim_elenco = (
    dim_elenco_local.unionByName(dim_elenco_tmdb, allowMissingColumns=True)
    .dropDuplicates(["id_filme", "nome_artista"])
)

# Gera id_elenco garantindo unicidade por filme + artista
window = Window.orderBy("id_filme", "nome_artista")
dim_elenco = dim_elenco.withColumn("id_elenco", F.row_number().over(window))

# Fato local
fato_local = (
    df_local.alias("l")
    .join(dim_filmes.alias("f"), F.col("l.id") == F.col("f.id_filme"))
    .join(dim_elenco.alias("e"), F.col("l.id") == F.col("e.id_filme"), "left")
    .join(dim_data.alias("d"), F.col("l.anoLancamento") == F.col("d.ano"), "left")
    .select(
        F.col("f.id_filme"),
        F.round(F.col("l.notaMedia").cast("float"), 1).alias("nota_filme"),
        F.lit(0.0).alias("popularidade_filme"),
        F.col("e.id_elenco"),
        F.col("d.id_data")
    )
    .dropDuplicates(["id_filme", "id_elenco", "id_data"]) # remove duplicatas
)

# Fato tmdb
fato_tmdb = (
    df_tmdb.alias("t")
    .join(dim_filmes.alias("f"), F.col("t.id").cast("string") == F.col("f.id_filme"))
    .join(dim_elenco.alias("e"), F.col("t.id").cast("string") == F.col("e.id_filme"), "left")
    .join(dim_data.alias("d"), F.year("t.release_date") == F.col("d.ano"), "left")
    .select(
        F.col("f.id_filme"),
        F.round(F.col("t.vote_average").cast("float"), 1).alias("nota_filme"),
        F.col("t.popularity").cast("float").alias("popularidade_filme"),
        F.col("e.id_elenco"),
        F.col("d.id_data")
    )
    .dropDuplicates(["id_filme", "id_elenco", "id_data"]) # remove duplicatas
)

# União das fatos
fato_avaliacoes_filme = (
    fato_local
    .unionByName(fato_tmdb)
    .dropDuplicates(["id_filme", "id_elenco", "id_data"])
)

target_path = args['S3_TARGET_PATH']

dim_filmes.write.mode("overwrite").parquet(f"{target_path}dim_filmes/")
dim_data.write.mode("overwrite").parquet(f"{target_path}dim_data/")
dim_elenco.write.mode("overwrite").parquet(f"{target_path}dim_elenco/")
fato_avaliacoes_filme.write.mode("overwrite").parquet(f"{target_path}fato_avaliacoes_filme/")

job.commit()