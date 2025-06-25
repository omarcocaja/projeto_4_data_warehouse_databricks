# Databricks notebook source
import dlt
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType

# Define o schema da tabela de vendas
schema_vendas = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("preco_unitario", DoubleType(), True),
    StructField("valor_total", DoubleType(), True),
    StructField("timestamp_venda", TimestampType(), True)
])

@dlt.table(
    name="bronze_vendas",
    comment="Dados brutos de vendas extraídos da camada landing e armazenados em formato Delta."
)
def bronze_vendas():
    return (
        spark.readStream  # ou read se for batch
        .format("cloudFiles")  # leitura contínua
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(schema_vendas)
        .load("/mnt/projeto-portfolio-quatro/datalake/landing/vendas")
    )
