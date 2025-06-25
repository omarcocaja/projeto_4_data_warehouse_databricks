# Databricks notebook source
import pandas as pd
import random
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DoubleType
import os
import pyspark.sql.functions as F

# COMMAND ----------

MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'
clientes_path = f"{MOUNT_DATALAKE}/datalake/landing/clientes"
produtos_path = f"/dbfs{MOUNT_DATALAKE}/datalake/landing/produtos/produtos.csv"
vendas_path = f"{MOUNT_DATALAKE}/datalake/landing/vendas"

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
    StructField("email", StringType(), True),
    StructField("timestamp_processamento", TimestampType(), True)
])

schema_vendas = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("preco_unitario", DoubleType(), True),
    StructField("valor_total", DoubleType(), True),
    StructField("timestamp_venda", TimestampType(), True)
])


# COMMAND ----------

# Injeção de parâmetros variáveis
data_hoje = datetime.now().strftime("%Y-%m-%d")
dbutils.widgets.text("data_input", data_hoje, "Data da Venda")

data_input = dbutils.widgets.get("data_input")

if data_input == "{{ds}}":
    data_base = datetime.today()
else:
    data_base = datetime.strptime(data_input, "%Y-%m-%d")


dbutils.widgets.text('volume_vendas', "100000", 'Volume de vendas')
volume_diario = int(dbutils.widgets.get('volume_vendas'))


# COMMAND ----------

df_clientes_spark = spark.read.option("header", "true").schema(schema_clientes).csv(clientes_path)
max_customer_id = int(df_clientes_spark.selectExpr("max(customer_id)").collect()[0][0])

df_produtos = pd.read_csv(produtos_path)
max_product_id = df_produtos["product_id"].max()
last_order_id = 0

try:
    arquivos_vendas = list(sorted([path.path for path in dbutils.fs.ls(vendas_path)]))
    if len(arquivos_vendas) > 0:
        df_vendas_mais_recente = spark.read.format("csv").option("header", "true").load(arquivos_vendas[-1])
        last_order_id = df_vendas_mais_recente.agg(F.max(F.col("order_id").cast("int"))).collect()[0][0]
        if last_order_id is None:
            last_order_id = 0
    else:
        last_order_id = 0
except Exception as e:
    if "FileNotFoundException" in str(e):
        print("Diretório de vendas não encontrado, criando um novo.")
        dbutils.fs.mkdirs(vendas_path)
        last_order_id = 0
    else:
        raise e

print('Data base: ', data_base.strftime("%Y-%m-%d"))
print('Último order_id: ', last_order_id)
print('Último customer_id: ', max_customer_id)
print('Último product_id: ', max_product_id)

# COMMAND ----------

vendas = []

for i in range(1, volume_diario + 1):
    order_id = last_order_id + i
    customer_id = random.randint(1, max_customer_id)
    product_id = random.randint(1, max_product_id)
    
    produto = df_produtos[df_produtos["product_id"] == product_id].iloc[0]
    preco = produto["preco"]
    quantidade = random.randint(1, 5)
    valor_total = round(preco * quantidade, 2)

    # Timestamp aleatório entre 08h e 22h
    hora = random.randint(8, 21)
    minuto = random.randint(0, 59)
    segundo = random.randint(0, 59)
    timestamp = datetime.combine(data_base.date(), datetime.min.time()) + timedelta(hours=hora, minutes=minuto, seconds=segundo)

    vendas.append([
        order_id, customer_id, product_id, quantidade,
        preco, valor_total, timestamp
    ])

df_vendas = pd.DataFrame(vendas, columns=[
    "order_id", "customer_id", "product_id", "quantidade",
    "preco_unitario", "valor_total", "timestamp_venda"
])

# COMMAND ----------

try:
    df_vendas.to_csv(f"/dbfs{MOUNT_DATALAKE}/datalake/landing/vendas/vendas_{data_base.strftime('%Y%m%d')}.csv", index=False)
except Exception as e:
    if 'non-existent directory' in str(e):
        os.makedirs(f"/dbfs{MOUNT_DATALAKE}/datalake/landing/vendas", exist_ok=True)
        df_vendas.to_csv(f"/dbfs{MOUNT_DATALAKE}/datalake/landing/vendas/vendas_{data_base.strftime('%Y%m%d')}.csv", index=False)    
    else:
        raise e




# COMMAND ----------

df_vendas_spark = spark.read.option("header", "true").schema(schema_vendas).csv(f"{MOUNT_DATALAKE}/datalake/landing/vendas/")

display(df_vendas_spark)