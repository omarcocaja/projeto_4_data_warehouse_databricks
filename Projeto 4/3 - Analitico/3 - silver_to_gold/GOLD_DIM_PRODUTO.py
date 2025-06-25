# Databricks notebook source
from pyspark.sql.functions import current_timestamp

MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'
dim_path = f"{MOUNT_DATALAKE}/datalake/gold/dim_produto"

df_dim_produto = (
    spark.read.format("delta").table('tb_sv_produtos')
    .select(
        "product_id",
        "nome_produto",
        "categoria",
        "preco_unitario"
    )
    .dropDuplicates(["product_id"])
    .withColumn("timestamp_insercao", current_timestamp())
)

df_dim_produto.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(dim_path)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tb_gd_dim_produto
# MAGIC USING DELTA
# MAGIC --LOCATION '/mnt/portfolio-quatro/datalake/gold/dim_produto';
# MAGIC