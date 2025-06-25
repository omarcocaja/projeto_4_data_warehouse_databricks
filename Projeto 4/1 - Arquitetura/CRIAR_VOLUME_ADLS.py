# Databricks notebook source
# MAGIC %pip install dotenv
# MAGIC from dotenv import load_dotenv, find_dotenv
# MAGIC import os
# MAGIC load_dotenv(find_dotenv())
# MAGIC

# COMMAND ----------

!databricks secrets create-scope portfolio-quatro

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

if 'projeto-portfolio-quatro' not in [s.name for s in w.secrets.list_scopes()]: w.secrets.create_scope(scope='projeto-portfolio-quatro', initial_manage_principal='users')

# COMMAND ----------

w.secrets.put_secret(
    'projeto-portfolio-quatro',
    'azure_client_id',
    string_value=os.getenv('CLIENT_ID')
)

w.secrets.put_secret(
    'projeto-portfolio-quatro',
    'azure_client_secret',
    string_value=os.getenv('CLIENT_SECRET')
)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

TENANT_ID_ENTRA = ''
CONTAINER_NAME = ''
STORAGE_ACCOUNT_NAME = ''
MOUNT_POINT = "/mnt/projeto-portfolio-quatro/"

configs = {
    'fs.azure.account.auth.type': 'OAuth',
    'fs.azure.account.oauth.provider.type': 'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
    'fs.azure.account.oauth2.client.id': dbutils.secrets.get(scope="projeto-portfolio-quatro", key="azure_client_id"),
    'fs.azure.account.oauth2.client.secret': dbutils.secrets.get(scope="projeto-portfolio-quatro", key="azure_client_secret"),
    'fs.azure.account.oauth2.client.endpoint': f'https://login.microsoftonline.com/{TENANT_ID_ENTRA}/oauth2/token'
}


# Unmount if the mount point already exists
if any(mount.mountPoint == MOUNT_POINT for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(MOUNT_POINT)

dbutils.fs.mount(
  source = "fabfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/",
  mount_point = MOUNT_POINT,
  extra_configs = configs
)

# COMMAND ----------

df = spark.createDataFrame([['A', 1], ['B', 2]])

(
    df
    .write
    .mode('overwrite')
    .save(f'{MOUNT_POINT}/datalake/bronze')
)