import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import input_file_name, from_json, explode, regexp_extract, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, ArrayType

args = getResolvedOptions(sys.argv, ['JOB_NAME','S3_INPUT_PATH','S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

movie_schema = StructType([
    StructField("id", LongType(), True),
    StructField("title", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", LongType(), True),
    StructField("genre_main", StringType(), True),
    StructField("genres", ArrayType(StringType()), True),
    StructField("keywords", ArrayType(StringType()), True),
    StructField("cast", ArrayType(
        StructType([
            StructField("name", StringType(), True),
            StructField("character", StringType(), True),
            StructField("gender", LongType(), True)
        ])
    ), True)
])

# Lê os arquivos JSON como texto
df_raw = spark.read.option("recursiveFileLookup", True).text(source_file)

# Converte para JSON usando o schema definido
df_json = df_raw.select(
    from_json(df_raw.value, ArrayType(movie_schema)).alias("filmes"),
    input_file_name().alias("file_path")
)

# Explode o array de filmes
df = df_json.select(explode("filmes").alias("row"), "file_path").select("row.*", "file_path")

df = df.withColumn("file_path", input_file_name())

# Adiciona ano, mês e dia extraídos do caminho
df = df.withColumn("ano", regexp_extract("file_path", r"/(\d{4})/", 1))
df = df.withColumn("mes", regexp_extract("file_path", r"/\d{4}/(\d{2})/", 1))
df = df.withColumn("dia", regexp_extract("file_path", r"/\d{4}/\d{2}/(\d{2})/", 1))

# Garante que as colunas de partição existam e sejam string
df = df.withColumn("genre_main", col("genre_main").cast("string"))
df = df.withColumn("ano", col("ano").cast("string"))
df = df.withColumn("mes", col("mes").cast("string"))
df = df.withColumn("dia", col("dia").cast("string"))

# Log de verificação
df.select("genre_main", "ano", "mes", "dia").distinct().show(10, truncate=False)

# Converte para DynamicFrame e escreve particionado
df_dyn = DynamicFrame.fromDF(df, glueContext, "df_dyn")
glueContext.write_dynamic_frame.from_options(
    frame=df_dyn,
    connection_type="s3",
    connection_options={
        "path": target_path,
        "partitionKeys": ["genre_main", "ano", "mes", "dia"]
    },
    format="parquet"
)

job.commit()