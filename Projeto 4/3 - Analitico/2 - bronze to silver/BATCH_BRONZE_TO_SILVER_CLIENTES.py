# Databricks notebook source
from pyspark.sql.functions import col, max as spark_max
from delta.tables import DeltaTable

MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'
bronze_path = f"{MOUNT_DATALAKE}/datalake/bronze/clientes"
silver_path = f"{MOUNT_DATALAKE}/datalake/silver/clientes"

# Tenta obter o maior timestamp da prata
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

# Transformações (adapte se necessário)
df_transformado = df_bronze.dropDuplicates(["customer_id"])

# MERGE INTO com controle de reprocessamento
if DeltaTable.isDeltaTable(spark, silver_path):
    silver_table = DeltaTable.forPath(spark, silver_path)

    (
        silver_table.alias("silver")
        .merge(
            df_transformado.alias("bronze"),
            "silver.customer_id = bronze.customer_id"
        )
        .whenMatchedUpdate(condition="bronze.timestamp_processamento > silver.timestamp_processamento", set={
            "nome": "bronze.nome",
            "idade": "bronze.idade",
            "genero": "bronze.genero",
            "estado_civil": "bronze.estado_civil",
            "cidade": "bronze.cidade",
            "estado": "bronze.estado",
            "data_nascimento": "bronze.data_nascimento",
            "cpf": "bronze.cpf",
            "email": "bronze.email",
            "timestamp_processamento": "bronze.timestamp_processamento"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_transformado.write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tb_sv_clientes
# MAGIC USING DELTA
# MAGIC --LOCATION '/mnt/projeto-portfolio-quatro/datalake/silver/clientes'; -- ADICIONAR MOUNT
# MAGIC