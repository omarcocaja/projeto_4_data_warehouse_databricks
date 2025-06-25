# Databricks notebook source
from pyspark.sql.functions import col, max as spark_max
from delta.tables import DeltaTable

MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'
bronze_path = f"{MOUNT_DATALAKE}/datalake/bronze/vendas"
silver_path = f"{MOUNT_DATALAKE}/datalake/silver/vendas"

# 1. Lê a Silver (se existir) para pegar o maior timestamp_processamento
try:
    df_silver = spark.read.format("delta").load(silver_path)
    max_ts = df_silver.agg(spark_max("timestamp_processamento")).collect()[0][0]
except:
    print("Silver não existe ainda. Processando tudo.")
    max_ts = None

# 2. Lê a Bronze com filtro incremental
df_bronze = spark.read.format("delta").load(bronze_path)
if max_ts:
    df_bronze = df_bronze.filter(col("timestamp_processamento") > max_ts)

# 3. Aplica transformações
df_transformado = (
    df_bronze
    .filter("valor_total > 0 AND quantidade > 0")
)

# 4. Cria ou faz MERGE na Silver
if DeltaTable.isDeltaTable(spark, silver_path):
    silver_table = DeltaTable.forPath(spark, silver_path)
    
    (
        silver_table.alias("silver")
        .merge(
            df_transformado.alias("bronze"),
            "silver.order_id = bronze.order_id"
        )
        .whenMatchedUpdate(condition="bronze.timestamp_processamento > silver.timestamp_processamento", set={
            "customer_id": "bronze.customer_id",
            "product_id": "bronze.product_id",
            "quantidade": "bronze.quantidade",
            "preco_unitario": "bronze.preco_unitario",
            "valor_total": "bronze.valor_total",
            "timestamp_venda": "bronze.timestamp_venda",
            "timestamp_processamento": "bronze.timestamp_processamento"
        })
        # .whenMatchedUpdateAll(condition="bronze.timestamp_processamento > silver.timestamp_processamento")
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_transformado.write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tb_sv_vendas
# MAGIC USING DELTA
# MAGIC --LOCATION '/mnt/portfolio-quatro/datalake/silver/vendas';
# MAGIC