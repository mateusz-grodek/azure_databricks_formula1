# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal

# COMMAND ----------

#Function to mount container
def mount_adls(storage_account_name,container_name):
    # get secret from Key Voult
    client_id = dbutils.secrets.get('formula1-secret','formula1-secret-client-id')
    client_secret = dbutils.secrets.get('formula1-secret','formula1-secret-client-secret')
    tenant_id=  dbutils.secrets.get('formula1-secret','formula1-secret-tenant-id')
    #set spark congif
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if any(mount.mountPoint==f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    #mount the storage acount container
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)
    display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('databricksdlmg', 'raw')
mount_adls('databricksdlmg', 'processed')
mount_adls('databricksdlmg', 'presentation')


