# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import *

# Schema original
schema_vendas = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("preco_unitario", DoubleType(), True),
    StructField("valor_total", DoubleType(), True),
    StructField("timestamp_venda", TimestampType(), True)
])

MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'
landing_path = f"{MOUNT_DATALAKE}/datalake/landing/vendas"
bronze_path = f"{MOUNT_DATALAKE}/datalake/bronze/vendas"

df_vendas = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(schema_vendas)
    .load(landing_path)
    .withColumn("timestamp_processamento", current_timestamp())
)

(
    df_vendas.writeStream
    .format("delta")
    .option("checkpointLocation", bronze_path + "/_checkpoint")
    .outputMode("append")
    .start(bronze_path)
)
