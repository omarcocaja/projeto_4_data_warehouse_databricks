# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

from faker import Faker
import pandas as pd
import random
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
import pyspark.sql.functions as F

MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'

PROCESSAMENTO = datetime.now()
DATA_HORA = PROCESSAMENTO.strftime('%Y%m%d%H%M%S')
fake = Faker('pt_BR')
last_id = 0
clientes_path = f"{MOUNT_DATALAKE}/datalake/landing/clientes"

# COMMAND ----------

try:
    arquivos_clientes = list(sorted([path.path for path in dbutils.fs.ls(clientes_path)]))
    if len(arquivos_clientes) > 0:
        df_cliente_mais_recente = spark.read.format('csv').option('header', 'true').load(arquivos_clientes[-1])
        last_id = df_cliente_mais_recente.agg(F.max(F.col('customer_id').cast('int'))).collect()[0][0]
except Exception as e:
    if 'FileNotFoundException' in str(e):
        print('Diretorio não encontrado, criando um novo.')
        dbutils.fs.mkdirs(clientes_path)
        df_cliente_mais_recente = pd.DataFrame()
    else:
        raise e

print(arquivos_clientes)


# COMMAND ----------

n_novos = random.randint(1, 100)

novos_clientes = []
for i in range(1, n_novos + 1):
    id_cliente = last_id + i
    nome = fake.name()
    idade = random.randint(18, 75)
    genero = random.choice(["Masculino", "Feminino"])
    estado_civil = random.choice(["Solteiro", "Casado", "Divorciado", "Viúvo"])
    cidade = fake.city()
    estado = fake.estado_sigla()
    nascimento = pd.Timestamp.now().normalize() - pd.to_timedelta(idade * 365, unit='D')
    cpf = fake.cpf()
    email = fake.email()
    timestamp_processamento = PROCESSAMENTO

    novos_clientes.append([id_cliente, nome, idade, genero, estado_civil, cidade, estado, nascimento.date(), cpf, email, timestamp_processamento])

df_novos_clientes = pd.DataFrame(novos_clientes, columns = [
    'customer_id', 'nome', 'idade', 'genero', 'estado_civil', 'cidade', 'estado', 'data_nascimento', 'cpf', 'email', 'timestamp_processamento'
])



# COMMAND ----------

try:
    df_novos_clientes.to_csv(f"/dbfs{clientes_path}/cadastro_clientes_{DATA_HORA}.csv", header=True, index=False)  

except Exception as e:
    if 'non-existent directory' in str(e):
        os.makedirs(f"/dbfs{clientes_path}", exist_ok=True)
        df_novos_clientes.to_csv(f"/dbfs{clientes_path}/cadastro_clientes_{DATA_HORA}.csv", header=True, index=False)
    else:
        raise e

# COMMAND ----------

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

display(
    spark.read
    .option('header', 'true')
    .schema(schema_clientes)
    .csv(clientes_path)
    .orderBy('customer_id', ascending=False)
)