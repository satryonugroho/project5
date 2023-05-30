#Import Module

import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os
import time
import pandas_gbq, pandas

#Start findspark
findspark.init()

#Create Spark Session
spark = SparkSession.builder \
    .appName("FHV Trip February 2021") \
    .master('local[*]') \
    .getOrCreate()

#Load Parquet file to dataframe
parquet_file = "fhv_tripdata_2021_02.parquet"
df_fhv = spark.read.parquet(parquet_file)

#2 Find the longest trip for each day ?
df_2 = df_fhv
date_format = "yyyy-MM-dd HH:mm:ss"  # Sesuaikan format ini jika format tanggal pada data Anda berbeda
df_2 = df_2.withColumn('pickup_datetime', to_timestamp(col('pickup_datetime'), date_format))
df_2 = df_2.withColumn('dropOff_datetime', to_timestamp(col('dropOff_datetime'), date_format))
df_2 = df_2.withColumn('trip_duration_minutes', (col('dropOff_datetime').cast("long") - col('pickup_datetime').cast("long")) / 60)
df_2 = df_2.withColumn('trip_date', to_date(col('pickup_datetime')))
window_spec = Window.partitionBy('trip_date').orderBy(col('trip_duration_minutes').desc())
df_2 = df_2.withColumn('row_number', row_number().over(window_spec))
longest_trip_each_day = df_2.select('trip_date', 'dispatching_base_num', 'trip_duration_minutes') \
    .filter(col('row_number') == 1) \
    .drop('row_number') \
    .sort('trip_date')

path_to_credentials = 'key.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credentials

project_id="data-engineer-digitalskola"
dataset_id="project5"


table_id="longest_trip_each_day"
destination_table = f"{dataset_id}.{table_id}"
pandas_df = longest_trip_each_day.toPandas()
pandas_df['trip_date'] = pandas_df['trip_date'].astype('string')
pandas_df['dispatching_base_num'] = pandas_df['dispatching_base_num'].astype('string')
pandas_gbq.to_gbq(pandas_df, destination_table, project_id=project_id, if_exists='replace')
print(pandas_df.dtypes)
