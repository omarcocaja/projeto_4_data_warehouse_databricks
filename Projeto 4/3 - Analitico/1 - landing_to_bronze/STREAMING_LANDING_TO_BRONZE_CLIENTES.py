# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp

# Schema da tabela de clientes
schema_clientes = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("idade", IntegerType(), True),
    StructField("genero", StringType(), True),
    StructField("estado_civil", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("data_nascimento", DateType(), True),
    StructField("cpf", StringType(), True),
    StructField("email", StringType(), True)
])
MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'
landing_path = f"{MOUNT_DATALAKE}/datalake/landing/clientes"
bronze_path = f"{MOUNT_DATALAKE}/datalake/bronze/clientes"

df_clientes = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(schema_clientes)
    .load(landing_path)
    .withColumn("timestamp_processamento", current_timestamp())
)

(
    df_clientes.writeStream
    .format("delta")
    .option("checkpointLocation", bronze_path + "/_checkpoint")
    .outputMode("append")
    .start(bronze_path)
)
