# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Containers for the Formula1 Project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # adding secrets from key vault
    client_id = dbutils.secrets.get(scope= 'Formula1 scope', key= 'formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope= 'Formula1 scope', key= 'Formula1-app-tenant-id')
    client_secret_value = dbutils.secrets.get(scope= 'Formula1 scope', key= 'Formula1-app-client-secret')
    
    # setting configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret_value,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # check to see if already mounted (unmounts & remounts if so)
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # mounting the specified account
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    # display mounts to confirm successful mount
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('kjmformula1dl','raw')

# COMMAND ----------

mount_adls('kjmformula1dl','presentation')

# COMMAND ----------

mount_adls('kjmformula1dl','processed')
