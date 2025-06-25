# Databricks notebook source
from pyspark.sql.functions import col, trunc, current_date, date_format
from datetime import datetime

PRIMEIRO_DIA_MES = datetime.today().strftime("%Y-%m-01")

df_resultado = spark.sql(f"""
SELECT DISTINCT
    v.order_id,
    v.customer_id,
    v.product_id,
    v.timestamp_venda,
    DATE(v.timestamp_venda) AS data_venda,
    v.quantidade,
    v.preco_unitario,
    v.valor_total,
    v.timestamp_processamento,
    current_timestamp() AS timestamp_carga
FROM (
  SELECT * FROM tb_sv_vendas WHERE DATE(timestamp_venda) >= DATE('{PRIMEIRO_DIA_MES}')
) v 
LEFT JOIN tb_gd_dim_cliente c ON v.customer_id = c.customer_id
LEFT JOIN tb_gd_dim_produto p ON v.product_id = p.product_id
LEFT JOIN (
  SELECT * FROM tb_gd_dim_tempo WHERE data >= DATE('{PRIMEIRO_DIA_MES}')
) t ON DATE(v.timestamp_venda) = t.data
""")

display(df_resultado)

# COMMAND ----------

MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'

df_resultado.write.format("delta").mode("overwrite").save(f"{MOUNT_DATALAKE}/datalake/gold/fato_vendas")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tb_gd_fato_vendas
# MAGIC USING DELTA
# MAGIC --LOCATION '/mnt/portfolio-quatro/datalake/gold/fato_vendas';
# MAGIC