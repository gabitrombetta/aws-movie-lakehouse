import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            "s3://datalake-gabrielatrombetta/Raw/Local/CSV/Movies/2025/10/09/movies.csv"
        ],
    },
    "csv",
    {"withHeader": True, "separator":"|"},
)

glueContext.write_dynamic_frame.from_options(
    frame = df,
    connection_type = "s3",
    connection_options = {
        "path": "s3://datalake-gabrielatrombetta/Trusted/Local/Parquet/Movies/",
        "partitionKeys": []
        },
    format = "parquet"
)

job.commit()