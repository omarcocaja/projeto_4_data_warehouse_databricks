# Databricks notebook source
from pyspark.sql.functions import col, year, month, dayofmonth, hour, date_format

MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'
silver_path = f"{MOUNT_DATALAKE}/datalake/silver/vendas"
dim_path = f"{MOUNT_DATALAKE}/datalake/gold/dim_tempo"

df_vendas = spark.read.format("delta").load(silver_path)

df_dim_tempo = (
    df_vendas
    .select("timestamp_venda")
    .dropDuplicates()
    .withColumn("data", col("timestamp_venda").cast("date"))
    .withColumn("ano", year("timestamp_venda"))
    .withColumn("mes", month("timestamp_venda"))
    .withColumn("dia", dayofmonth("timestamp_venda"))
    .withColumn("hora", hour("timestamp_venda"))
    .withColumn("dia_semana", date_format("timestamp_venda", "E"))
)

df_dim_tempo.write.format("delta").mode("overwrite").save(dim_path)
# .saveAsTable('dim_tempo')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tb_gd_dim_tempo
# MAGIC USING DELTA
# MAGIC --LOCATION '/mnt/portfolio-quatro/datalake/gold/dim_tempo';
# MAGIC