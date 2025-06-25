# Databricks notebook source
import pandas as pd
import random
MOUNT_DATALAKE = '/mnt/projeto-portfolio-quatro'

categorias = {
    "eletronicos": {"marcas": ["TechOne", "SmartPlus", "VisionTech", "LG", "Samsung"], "preco_min": 500, "preco_max": 8000},
    "eletrodomesticos": {"marcas": ["Frigelux", "CozinhaTop", "CleanBot", "Eletrolux"], "preco_min": 200, "preco_max": 4000},
    "acessorios": {"marcas": ["KeyLab", "SoundWave"], "preco_min": 50, "preco_max": 800},
    "informatica": {"marcas": ["PrintMax", "NetCore"], "preco_min": 300, "preco_max": 3000},
    "moveis": {"marcas": ["ErgoPlay"], "preco_min": 500, "preco_max": 1500}
}


# COMMAND ----------

def gerar_nome(categoria, marca):
    base_nomes = {
        "eletronicos": ["Smartphone", "Notebook", "TV 4K", "Tablet", "Headphone"],
        "eletrodomesticos": ["Geladeira", "Cafeteira", "Micro-ondas", "Aspirador Robô", "Airfryer"],
        "acessorios": ["Mouse", "Teclado", "Fone de Ouvido", "Carregador", "Cabo USB"],
        "informatica": ["Monitor", "Roteador", "Impressora", "Scanner", "Webcam"],
        "moveis": ["Cadeira Gamer", "Mesa de Escritório", "Estante", "Gabinete"]
    }
    nome_base = random.choice(base_nomes[categoria])
    sufixo = random.choice(["X", "Plus", "Pro", "2024", "Max", "Ultra"])
    return f"{nome_base} {sufixo} – {marca}"


# COMMAND ----------



# Geração dos produtos
produtos = []
n = 150  # número de produtos
for i in range(1, n + 1):
    categoria = random.choice(list(categorias.keys()))
    marca = random.choice(categorias[categoria]["marcas"])
    preco = round(random.uniform(categorias[categoria]["preco_min"], categorias[categoria]["preco_max"]), 2)
    nome = gerar_nome(categoria, marca)
    produtos.append({"product_id": i, "nome": nome, "categoria": categoria, "preco": preco, "marca": marca})

df_produtos_dinamicos = pd.DataFrame(produtos)


# COMMAND ----------

import pyspark.pandas as ps

(
    df_produtos_dinamicos
    .to_csv(f"/dbfs{MOUNT_DATALAKE}/datalake/landing/produtos/produtos.csv", header=True, index=False)
)


# COMMAND ----------

import pandas as pd

display(
    pd.read_csv(f'/dbfs{MOUNT_DATALAKE}/datalake/landing/produtos/produtos.csv')
)