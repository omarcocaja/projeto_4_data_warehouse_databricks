# Databricks notebook source
from pyspark.sql.functions import col, max as spark_max
from delta.tables import DeltaTable

MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'
bronze_path = f"{MOUNT_DATALAKE}/datalake/bronze/produtos"
silver_path = f"{MOUNT_DATALAKE}/datalake/silver/produtos"

# Pega maior timestamp da silver (se existir)
try:
    df_silver = spark.read.format("delta").load(silver_path)
    max_ts = df_silver.agg(spark_max("timestamp_processamento")).collect()[0][0]
except:
    print("Silver ainda não existe. Processando tudo.")
    max_ts = None

# Leitura da bronze com filtro incremental
df_bronze = spark.read.format("delta").load(bronze_path)
if max_ts:
    df_bronze = df_bronze.filter(col("timestamp_processamento") > max_ts)

# Transformações (simples para produtos)
df_transformado = df_bronze.dropDuplicates(["product_id"])

# MERGE INTO na silver
if DeltaTable.isDeltaTable(spark, silver_path):
    silver_table = DeltaTable.forPath(silver_path)

    (
        silver_table.alias("silver")
        .merge(
            df_transformado.alias("bronze"),
            "silver.product_id = bronze.product_id"
        )
        .whenMatchedUpdateAll(condition="bronze.timestamp_processamento > silver.timestamp_processamento")
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_transformado.write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tb_sv_produtos
# MAGIC USING DELTA
# MAGIC --LOCATION '/mnt/portfolio-quatro/datalake/silver/produtos';
# MAGIC