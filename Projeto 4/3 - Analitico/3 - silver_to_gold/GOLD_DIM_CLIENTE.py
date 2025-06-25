# Databricks notebook source
from pyspark.sql.functions import current_timestamp

MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'
silver_path = f"{MOUNT_DATALAKE}/datalake/silver/clientes"
dim_path = f"{MOUNT_DATALAKE}/datalake/gold/dim_cliente"

df_dim_cliente = (
    spark.read.format("delta").load(silver_path)
    .select(
        "customer_id",
        "nome",
        "idade",
        "genero",
        "estado_civil",
        "cidade",
        "estado"
    )
    .dropDuplicates(["customer_id"])
    .withColumn("timestamp_insercao", current_timestamp())
)

df_dim_cliente.write.format("delta").mode("overwrite").save(dim_path)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tb_gd_dim_cliente
# MAGIC USING DELTA
# MAGIC --LOCATION '/mnt/portfolio-quatro/datalake/gold/dim_cliente';
# MAGIC