# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp

# Schema da tabela de produtos
schema_produtos = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("nome_produto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("preco_unitario", DoubleType(), True),
])

MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'
landing_path = f"{MOUNT_DATALAKE}/datalake/landing/produtos"
bronze_path = f"{MOUNT_DATALAKE}/datalake/bronze/produtos"

df_produtos = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(schema_produtos)
    .load(landing_path)
    .withColumn("timestamp_processamento", current_timestamp())
)

(
    df_produtos.writeStream
    .format("delta")
    .option("checkpointLocation", bronze_path + "/_checkpoint")
    .outputMode("append")
    .start(bronze_path)
)
