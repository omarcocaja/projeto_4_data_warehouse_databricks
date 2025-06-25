# Databricks notebook source
import dlt
from pyspark.sql.types import *

schema_produtos = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("preco", DoubleType(), True),
    StructField("marca", StringType(), True)
])

@dlt.table(
    name="bronze_produtos",
    comment="Dados brutos de produtos vindos da camada landing"
)
def bronze_produtos():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(schema_produtos)
        .load("/mnt/projeto-portfolio-quatro/datalake/landing/produtos")
    )
