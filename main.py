from google.cloud import bigquery
from google.oauth2 import service_account
import os
from pyspark.sql import SparkSession

# Ganti path ini dengan lokasi file kredensial JSON Anda
path_to_credentials = 'key.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credentials

# Membuat kredensial dari file JSON
credentials = service_account.Credentials.from_service_account_file(path_to_credentials)

# Membuat klien BigQuery
bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Tulis SQL query Anda
sql_query = """
    SELECT *
    FROM `data-engineer-digitalskola.playground.raw_netflix_title`
    LIMIT 10
"""

spark = SparkSession.builder \
    .appName("PySpark BigQuery Example") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.21.1") \
    .getOrCreate()
