# Databricks notebook source
# MAGIC %md
# MAGIC ### Evolução da Receita Mensal
# MAGIC
# MAGIC Analisar a receita total por mês nos ajuda a identificar tendências, sazonalidade e momentos de alta ou baixa performance.
# MAGIC
# MAGIC Abaixo, agrupamos as vendas por mês (`ano_mes`) e somamos o valor total das vendas.
# MAGIC

# COMMAND ----------

# Importações
from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.ml.clustering import KMeans
import numpy as np

MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'

# Estilo visual
plt.style.use("ggplot")
sns.set(rc={"figure.figsize": (12, 6)})

df_vendas = spark.table('tb_gd_fato_vendas')

# Agrupamento por ano-mês
df_receita_mensal = (
    df_vendas
    .withColumn("ano_mes", F.date_format("timestamp_venda", "yyyy-MM"))
    .groupBy("ano_mes")
    .agg(F.sum("valor_total").alias("receita_total"))
    .orderBy("ano_mes")
)

# Conversão controlada para Pandas (pequeno volume agregado)
pdf_receita_mensal = df_receita_mensal.toPandas()

# Gráfico
plt.figure()
sns.lineplot(data=pdf_receita_mensal, x='ano_mes', y='receita_total', marker='o')
plt.title('Evolução da Receita Mensal')
plt.xlabel('Mês')
plt.ylabel('Receita Total (R$)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Volume de Pedidos por Dia
# MAGIC
# MAGIC Esta visualização ajuda a entender o comportamento diário de vendas. Picos podem indicar datas promocionais, enquanto quedas podem revelar problemas operacionais ou baixa demanda.
# MAGIC
# MAGIC Agrupamos os pedidos por data (`timestamp_venda`) e contamos o número de pedidos (`order_id`).
# MAGIC

# COMMAND ----------

# Estilo visual
plt.style.use("ggplot")
sns.set(rc={"figure.figsize": (12, 6)})

# Agrupamento por data (conversão para data sem hora)
df_pedidos_por_dia = (
    df_vendas
    .withColumn("data_venda", F.to_date("timestamp_venda"))
    .groupBy("data_venda")
    .agg(F.countDistinct("order_id").alias("total_pedidos"))
    .orderBy("data_venda")
)

# Conversão para Pandas
pdf_pedidos_por_dia = df_pedidos_por_dia.toPandas()

# Plot
plt.figure()
sns.lineplot(data=pdf_pedidos_por_dia, x='data_venda', y='total_pedidos')
plt.title('Número de Pedidos por Dia')
plt.xlabel('Data')
plt.ylabel('Total de Pedidos')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ----

# COMMAND ----------

# MAGIC %md
# MAGIC ### Produtos Mais Vendidos (em Volume)
# MAGIC
# MAGIC Analisar os produtos com maior volume de vendas ajuda a identificar o que tem maior saída. Isso é essencial para planejamento de estoque e campanhas de marketing.
# MAGIC
# MAGIC Aqui agrupamos os dados por `product_id` e somamos as `quantidades`.
# MAGIC

# COMMAND ----------

# Top 10 produtos mais vendidos por quantidade
df_top_produtos_qtd = (
    df_vendas
    .groupBy("product_id")
    .agg(F.sum("quantidade").alias("total_vendido"))
    .orderBy(F.desc("total_vendido"))
    .limit(10)
)

# Conversão para Pandas
pdf_top_produtos_qtd = df_top_produtos_qtd.toPandas()

# Plot
plt.figure()
sns.barplot(data=pdf_top_produtos_qtd, x="total_vendido", y="product_id", orient="h")
plt.title("Top 10 Produtos Mais Vendidos (Quantidade)")
plt.xlabel("Quantidade Total Vendida")
plt.ylabel("ID do Produto")
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Receita Total por Produto
# MAGIC
# MAGIC Nem sempre o produto mais vendido em volume é o que gera maior receita. Essa análise mostra quais produtos mais contribuem para o faturamento.
# MAGIC
# MAGIC Agrupamos por `product_id` e somamos `valor_total`.
# MAGIC

# COMMAND ----------

df_top_produtos_valor = (
    df_vendas
    .groupBy("product_id")
    .agg(F.sum("valor_total").alias("receita_total"))
    .orderBy(F.desc("receita_total"))
    .limit(10)
)

# Conversão para Pandas
pdf_top_produtos_valor = df_top_produtos_valor.toPandas()

# Plot
plt.figure()
sns.barplot(data=pdf_top_produtos_valor, x="receita_total", y="product_id", orient="h")
plt.title("Top 10 Produtos por Receita Total")
plt.xlabel("Receita Total (R$)")
plt.ylabel("ID do Produto")
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Ticket Médio por Produto
# MAGIC
# MAGIC O ticket médio mostra quanto, em média, o cliente paga por cada produto. Produtos com ticket médio alto podem indicar oportunidades de vendas consultivas ou premium.
# MAGIC
# MAGIC Aqui usamos: ticket_medio = receita / quantidade
# MAGIC

# COMMAND ----------

# Cálculo do ticket médio por produto
df_ticket_medio = (
    df_vendas
    .groupBy("product_id")
    .agg(
        F.sum("valor_total").alias("valor_total"),
        F.sum("quantidade").alias("quantidade_total")
    )
    .withColumn("ticket_medio", F.col("valor_total") / F.col("quantidade_total"))
    .orderBy(F.desc("ticket_medio"))
    .limit(10)
)

# Conversão para Pandas
pdf_ticket_medio = df_ticket_medio.toPandas()

# Plot
plt.figure()
sns.barplot(data=pdf_ticket_medio, x='product_id', y='ticket_medio')
plt.title('Top 10 Produtos com Maior Ticket Médio')
plt.xlabel('ID do Produto')
plt.ylabel('Ticket Médio (R$)')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distribuição de Compras por Faixa Etária
# MAGIC
# MAGIC Analisar a idade dos clientes que mais compram ajuda a direcionar estratégias específicas para cada público. Agrupamos os clientes por faixas etárias e somamos o valor total comprado.
# MAGIC

# COMMAND ----------

# Leitura da dimensão clientes
df_clientes = (
    spark.read
    .format("delta")
    .load(f"{MOUNT_DATALAKE}/datalake/gold/dim_cliente")
)

# Enriquecendo a fato com informações de idade
df_vendas_enriquecida = (
    df_vendas.join(df_clientes.select("customer_id", "idade"), on="customer_id", how="inner")
)

# Criação de faixas etárias
df_vendas_faixa = (
    df_vendas_enriquecida
    .withColumn("faixa_etaria", F.when(F.col("idade") < 20, "<20")
                .when((F.col("idade") >= 20) & (F.col("idade") < 30), "20-29")
                .when((F.col("idade") >= 30) & (F.col("idade") < 40), "30-39")
                .when((F.col("idade") >= 40) & (F.col("idade") < 50), "40-49")
                .when((F.col("idade") >= 50) & (F.col("idade") < 60), "50-59")
                .otherwise("60+"))
    .groupBy("faixa_etaria")
    .agg(F.sum("valor_total").alias("receita_total"))
    .orderBy("faixa_etaria")
)

# Conversão para Pandas
pdf_faixa = df_vendas_faixa.toPandas()

# Plot
plt.figure()
sns.barplot(data=pdf_faixa, x="faixa_etaria", y="receita_total")
plt.title("Receita por Faixa Etária")
plt.xlabel("Faixa Etária")
plt.ylabel("Receita Total (R$)")
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Consumo por Gênero e Estado Civil
# MAGIC
# MAGIC Mapear o comportamento de consumo por gênero e estado civil pode revelar públicos mais engajados ou negligenciados nas campanhas.
# MAGIC

# COMMAND ----------

# Juntando cliente com fato para trazer genero e estado_civil
df_vendas_clientes = (
    df_vendas.join(
        df_clientes.select("customer_id", "genero", "estado_civil"),
        on="customer_id",
        how="inner"
    )
)

# Conversão para Pandas para visualização de distribuição
pdf_vendas_clientes = df_vendas_clientes.select(
    "genero", "estado_civil", "valor_total"
).toPandas()

# Boxplot por gênero
plt.figure()
sns.boxplot(data=pdf_vendas_clientes, x='genero', y='valor_total')
plt.title('Distribuição do Valor Comprado por Gênero')
plt.ylabel('Valor Total (R$)')
plt.tight_layout()
plt.show()

# Boxplot por estado civil
plt.figure()
sns.boxplot(data=pdf_vendas_clientes, x='estado_civil', y='valor_total')
plt.title('Distribuição do Valor Comprado por Estado Civil')
plt.ylabel('Valor Total (R$)')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Compras por Estado (UF)
# MAGIC
# MAGIC Quais estados mais consomem? Este gráfico mostra a distribuição de receita por estado.
# MAGIC

# COMMAND ----------

# Juntando com estado
df_vendas_estado = (
    df_vendas.join(df_clientes.select("customer_id", "estado"), on="customer_id", how="inner")
    .groupBy("estado")
    .agg(F.sum("valor_total").alias("receita_total"))
    .orderBy(F.desc("receita_total"))
    .limit(10)
)

# Pandas e plot
pdf_estado = df_vendas_estado.toPandas()

plt.figure()
sns.barplot(data=pdf_estado, x="receita_total", y="estado", orient="h")
plt.title("Top 10 Estados por Receita")
plt.xlabel("Receita Total (R$)")
plt.ylabel("Estado")
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Volume de Vendas por Hora do Dia
# MAGIC
# MAGIC Essa análise mostra em quais horários do dia os clientes mais compram. Útil para otimizar campanhas, suporte e estoque em tempo real.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import hour

# Extraindo hora da venda
df_por_hora = df_vendas.withColumn("hora", hour("timestamp_venda"))

# Contagem de pedidos por hora
df_vendas_hora = (
    df_por_hora.groupBy("hora")
    .agg(F.count("order_id").alias("total_pedidos"))
    .orderBy("hora")
)

# Pandas para visualização
pdf_vendas_hora = df_vendas_hora.toPandas()

# Gráfico
plt.figure()
sns.lineplot(data=pdf_vendas_hora, x='hora', y='total_pedidos', marker='o')
plt.title('Total de Pedidos por Hora do Dia')
plt.xlabel('Hora')
plt.ylabel('Número de Pedidos')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Volume de Vendas por Dia da Semana
# MAGIC
# MAGIC Visualizar as vendas ao longo dos dias da semana ajuda a descobrir padrões semanais. Isso pode apoiar decisões logísticas ou de campanha.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import dayofweek

# dayofweek: 1 = domingo, 2 = segunda, ..., 7 = sábado
dias = ['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb']
dia_map = {1: 'Dom', 2: 'Seg', 3: 'Ter', 4: 'Qua', 5: 'Qui', 6: 'Sex', 7: 'Sáb'}

df_dia_semana = df_vendas.withColumn("dia_semana", dayofweek("timestamp_venda"))
df_dia_semana = df_dia_semana.withColumn("nome_dia", F.create_map([F.lit(x) for x in sum(dia_map.items(), ())])[F.col("dia_semana")])

# Soma da receita por dia
df_vendas_dia = (
    df_dia_semana.groupBy("nome_dia")
    .agg(F.sum("valor_total").alias("valor_total"))
)

# Ordenação correta
pdf_vendas_dia = df_vendas_dia.toPandas().set_index("nome_dia").reindex(dias).reset_index()

# Gráfico
plt.figure()
sns.barplot(data=pdf_vendas_dia, x='nome_dia', y='valor_total')
plt.title('Receita Total por Dia da Semana')
plt.xlabel('Dia')
plt.ylabel('Receita (R$)')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Receita Total por Mês
# MAGIC
# MAGIC Essa análise mostra como a receita varia ao longo dos meses. Ajuda a identificar sazonalidade e datas importantes para o negócio.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import month

meses = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
         'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
mes_map = {i + 1: meses[i] for i in range(12)}

df_mes = df_vendas.withColumn("mes", month("timestamp_venda"))
df_mes = df_mes.withColumn("nome_mes", F.create_map([F.lit(x) for x in sum(mes_map.items(), ())])[F.col("mes")])

df_vendas_mes = (
    df_mes.groupBy("nome_mes")
    .agg(F.sum("valor_total").alias("valor_total"))
)

# Ordenar
pdf_vendas_mes = df_vendas_mes.toPandas().set_index("nome_mes").reindex(meses).reset_index()

# Gráfico
plt.figure()
sns.barplot(data=pdf_vendas_mes, x='nome_mes', y='valor_total')
plt.title('Receita Total por Mês')
plt.xlabel('Mês')
plt.ylabel('Receita (R$)')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Recência (Dias desde Última Compra)

# COMMAND ----------

# Última compra por cliente
df_ultimas_compras = (
    df_vendas.groupBy("customer_id")
    .agg(F.max("timestamp_venda").alias("ultima_compra"))
)

df_recencia = df_ultimas_compras.withColumn(
    "recencia_dias", F.datediff(F.current_date(), F.to_date("ultima_compra"))
)

pdf_recencia = df_recencia.select("recencia_dias").toPandas()

# Gráfico
plt.figure()
sns.histplot(pdf_recencia['recencia_dias'], bins=20, kde=True)
plt.title('Distribuição da Recência (dias desde a última compra)')
plt.xlabel('Recência (dias)')
plt.ylabel('Número de Clientes')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Frequência (Total de Compras por Cliente)

# COMMAND ----------

df_frequencia = (
    df_vendas.groupBy("customer_id")
    .agg(F.countDistinct("order_id").alias("frequencia_compras"))
)

pdf_frequencia = df_frequencia.toPandas()

plt.figure()
sns.histplot(pdf_frequencia['frequencia_compras'], bins=20)
plt.title('Distribuição da Frequência de Compras por Cliente')
plt.xlabel('Número de Compras')
plt.ylabel('Quantidade de Clientes')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Jovens compram produtos mais baratos?
# MAGIC Hipótese: Existe uma correlação entre a idade do cliente e o valor médio dos produtos comprados.

# COMMAND ----------

df_idade_valor = (
    df_vendas
    .join(
        spark.read.load(f'{MOUNT_DATALAKE}/datalake/gold/dim_cliente'), 'customer_id', 'inner'
    )
    .select("idade", "valor_total", "quantidade")
    .withColumn("ticket_medio", F.col("valor_total") / F.col("quantidade"))
)

pdf_idade_valor = df_idade_valor.select("idade", "ticket_medio").toPandas()

plt.figure()
sns.scatterplot(data=pdf_idade_valor, x="idade", y="ticket_medio", alpha=0.3)
plt.title('Relação entre Idade e Ticket Médio por Produto')
plt.xlabel('Idade do Cliente')
plt.ylabel('Ticket Médio (R$)')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Há diferença de consumo entre gêneros?
# MAGIC Hipótese: O comportamento de consumo varia significativamente entre gêneros, em valor ou frequência.

# COMMAND ----------

pdf_genero = (
    df_vendas
    .join(
        spark.read.load(f'{MOUNT_DATALAKE}/datalake/gold/dim_cliente'), 'customer_id', 'inner'
    )
    .select("genero", "valor_total")
    .toPandas()
)

plt.figure()
sns.boxplot(data=pdf_genero, x='genero', y='valor_total')
plt.title('Distribuição de Valor de Compras por Gênero')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Certas categorias têm melhor performance em certos Estados?
# MAGIC Hipótese: Alguns produtos têm maior aderência regional, o que pode orientar campanhas localizadas.

# COMMAND ----------

df_categorias_estados = (
    df_vendas
    .join(
        spark.read.load(f'{MOUNT_DATALAKE}/datalake/gold/dim_cliente'), 'customer_id', 'inner'
    )
    .groupBy("estado", "product_id")
    .agg(F.sum("valor_total").alias("receita_total"))
)

pdf_categorias_estados = df_categorias_estados.toPandas()

plt.figure(figsize=(14, 6))
sns.boxplot(data=pdf_categorias_estados, x="estado", y="receita_total")
plt.title('Receita por Produto em cada Estado')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Existem horários ideais para promoções?
# MAGIC Hipótese: O comportamento de compra ao longo do dia pode revelar janelas de alto impacto promocional.

# COMMAND ----------

df_horario_ticket = df_vendas.withColumn("hora", hour("timestamp_venda"))
df_horario_agg = (
    df_horario_ticket.groupBy("hora")
    .agg(F.count("order_id").alias("pedidos"), F.sum("valor_total").alias("receita"))
)

pdf_horario = df_horario_agg.toPandas()

# Gráfico
fig, ax1 = plt.subplots()

sns.lineplot(data=pdf_horario, x="hora", y="pedidos", marker='o', ax=ax1, label="Pedidos")
ax2 = ax1.twinx()
sns.lineplot(data=pdf_horario, x="hora", y="receita", marker='o', ax=ax2, color="orange", label="Receita")

ax1.set_title("Pedidos e Receita por Hora do Dia")
ax1.set_xlabel("Hora")
ax1.set_ylabel("Número de Pedidos")
ax2.set_ylabel("Receita (R$)")
fig.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Objetivo 1: Prever o ticket médio com base em dados demográficos
# MAGIC Problema: Podemos prever o valor médio gasto por um cliente a partir de atributos como idade, estado civil e gênero?

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
import pandas as pd
import matplotlib.pyplot as plt

# 1. Prepara os dados
df_ml = (
    df_vendas
    .join(
        spark.read.load(f'{MOUNT_DATALAKE}/datalake/gold/dim_cliente'), 'customer_id', 'inner'
    )
    .select("idade", "genero", "estado_civil", "valor_total", "quantidade")
    .withColumn("ticket_medio", F.col("valor_total") / F.col("quantidade"))
    .dropna()
)

# 2. Indexa colunas categóricas
indexers = [
    StringIndexer(inputCol="genero", outputCol="genero_idx", handleInvalid="keep"),
    StringIndexer(inputCol="estado_civil", outputCol="estado_civil_idx", handleInvalid="keep")
]

# 3. Vetor de features
assembler = VectorAssembler(
    inputCols=["idade", "genero_idx", "estado_civil_idx"],
    outputCol="features"
)

# 4. Modelo
lr = LinearRegression(featuresCol="features", labelCol="ticket_medio")

# 5. Pipeline
pipeline = Pipeline(stages=indexers + [assembler, lr])
modelo = pipeline.fit(df_ml)
avaliacao = modelo.stages[-1].summary

print(f"R²: {avaliacao.r2:.4f}")
print(f"RMSE: {avaliacao.rootMeanSquaredError:.2f}")

# Cria um DataFrame com idades de 18 a 80 e valores fixos de gênero e estado civil
idades = list(range(18, 81))
df_pred = spark.createDataFrame(
    [(idade, "Masculino", "Solteiro") for idade in idades],
    ["idade", "genero", "estado_civil"]
)

# Aplica o mesmo pipeline (sem re-treinar)
df_pred_transformado = modelo.transform(df_pred)

# Coleta para visualização
pdf_pred = (
    df_pred_transformado
    .select("idade", "prediction")
    .orderBy("idade")
    .toPandas()
)

# Gráfico
plt.figure(figsize=(10, 5))
plt.plot(pdf_pred["idade"], pdf_pred["prediction"], marker='o')
plt.title("Previsão de Ticket Médio por Idade (Masculino, Solteiro)")
plt.xlabel("Idade")
plt.ylabel("Ticket Médio Previsto (R$)")
plt.grid(True)
plt.tight_layout()
plt.show()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Objetivo 2: Agrupar perfis de clientes via clusterização (K-Means)
# MAGIC Problema: Podemos identificar grupos distintos de clientes com base em idade, frequência e ticket médio?

# COMMAND ----------


# 1. Calcula métricas por cliente
df_cluster = (
    df_vendas
    .join(
        spark.read.load(f'{MOUNT_DATALAKE}/datalake/gold/dim_cliente'), 'customer_id', 'inner'
    )
    .groupBy("customer_id", "idade")
    .agg(
        F.countDistinct("order_id").alias("frequencia"),
        (F.sum("valor_total") / F.sum("quantidade")).alias("ticket_medio")
    )
    .dropna()
)

# 2. Vetor
assembler = VectorAssembler(
    inputCols=["idade", "frequencia", "ticket_medio"],
    outputCol="features"
)

df_cluster_final = assembler.transform(df_cluster)

# 3. KMeans
kmeans = KMeans(k=4, seed=1)
modelo_kmeans = kmeans.fit(df_cluster_final)
resultado = modelo_kmeans.transform(df_cluster_final)

# 4. Visualização
pdf_clusters = resultado.select("customer_id", "features", "prediction").toPandas()


# Converte os vetores em colunas separadas
features_array = np.array(pdf_clusters['features'].tolist())

pdf_clusters['idade'] = features_array[:, 0]
pdf_clusters['frequencia'] = features_array[:, 1]
pdf_clusters['ticket_medio'] = features_array[:, 2]

plt.figure()
sns.scatterplot(
    data=pdf_clusters,
    x='frequencia',
    y='ticket_medio',
    hue='prediction',
    palette='Set2'
)
plt.title('Segmentação de Clientes com K-Means')
plt.tight_layout()
plt.show()
