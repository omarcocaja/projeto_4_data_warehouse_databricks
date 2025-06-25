# Data Warehouse com Databricks e Delta Live Tables

Este projeto simula um pipeline de dados completo no Databricks, desde a ingestão de dados transacionais até a modelagem analítica em um data warehouse. A arquitetura é organizada em camadas **Landing → Bronze → Silver → Gold**, utilizando recursos como **Delta Lake**, **Delta Live Tables**, **Structured Streaming** e **Volumes do Unity Catalog**.

---

## 🗂 Estrutura do Projeto

```
projeto_4_data_warehouse_databricks/
│
├── Projeto 4/
│   ├── 1 - Arquitetura/
│   │   ├── CRIAR_VOLUME_ADLS.py              # Script para criação de volume no ADLS (Unity Catalog)
│   │   ├── Projeto 4 - DER.pdf               # Diagrama Entidade-Relacionamento (modelo dimensional)
│   │   └── SCRIPT_DBDIAGRAM_IO               # Código usado no dbdiagram.io
│
│   ├── 2 - Transacional/
│   │   ├── GERAR_CLIENTES.py                 # Geração de dados simulados de clientes
│   │   ├── GERAR_PRODUTOS.py                 # Geração de produtos
│   │   └── GERAR_VENDAS.py                   # Geração de vendas com datas e valores
│
│   ├── 3 - Analitico/
│   │   ├── 1 - landing_to_bronze/
│   │   │   ├── DLT_LANDING_BRONZE_*.py       # DLT para ingestão batch
│   │   │   └── STREAMING_LANDING_TO_BRONZE_*.py # Ingestão streaming
│   │   ├── 2 - bronze to silver/
│   │   │   └── BATCH_BRONZE_TO_SILVER_*.py   # Transformações batch bronze → silver
│   │   └── 3 - silver_to_gold/
│   │       └── GOLD_DIM_*.py                 # Criação das dimensões e fatos finais
│
├── README.md                                 # Este arquivo
└── upload_this_file.zip                      # ZIP com o projeto para importar no Databricks
```

---

## ⚙️ Como Executar no Databricks

### 1. Crie um cluster com suporte a Delta Live Tables

- Mínimo recomendado: **DBR 13.3 LTS**, **Runtime Photon**, habilitar **Unity Catalog**.

### 2. Faça o upload do projeto

- Utilize o arquivo `upload_this_file.zip` para importar todos os notebooks e scripts de uma vez.

### 3. Crie um Volume no Unity Catalog

Execute o script `CRIAR_VOLUME_ADLS.py` para criar a estrutura necessária no seu workspace.

### 4. Gere os dados simulados

Execute os notebooks de `2 - Transacional` para gerar os arquivos CSV na camada `landing`.

### 5. Execute os pipelines analíticos

Use os scripts de Delta Live Tables e notebooks para processar os dados até a camada gold.

---

## 📦 Tecnologias Utilizadas

- **Databricks (Unity Catalog + Volumes + DLT)**
- **Apache Spark (Structured Streaming)**
- **Delta Lake**
- **Python 3.10+**

---

## 📄 Licença

Este projeto está licenciado sob a licença **MIT** e pode ser utilizado livremente para fins educacionais e comerciais.

---

## 📬 Contato

- [LinkedIn](https://www.linkedin.com/in/marco-caja)  
- [Instagram](https://www.instagram.com/omarcocaja)
