# Databricks notebook source
import dlt
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

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

@dlt.table(
    name="bronze_clientes",
    comment="Dados brutos de clientes vindos da camada landing"
)
def bronze_clientes():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(schema_clientes)
        .load("/mnt/projeto-portfolio-quatro/datalake/landing/clientes")
    )
