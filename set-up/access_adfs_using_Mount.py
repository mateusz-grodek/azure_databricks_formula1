# Databricks notebook source
# MAGIC  %md
# MAGIC  ### Mount Azure Data Lake using Service Principal
# MAGIC  #### Steps to follow
# MAGIC  1. Get client_id, tenant_id and client_secret from key vault
# MAGIC  2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC  3. Call file system utlity mount to mount the storage
# MAGIC  4. Explore other file system utlities related to mount (list all mounts, unmount)

# COMMAND ----------

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

dbutils.fs.unmount(f"/mnt/databricksdlmg/proccess")

# COMMAND ----------



mount_adls('databricksdlmg', 'raw')
mount_adls('databricksdlmg', 'processed')
mount_adls('databricksdlmg', 'presentation')


# COMMAND ----------

# client_id = "App id from app registration"
# tenant_id ="tenet id from app registration"
# client_secret = "secrets from app registration"
client_id = dbutils.secrets.get('formula1-secret','formula1-secret-client-id')
client_secret = dbutils.secrets.get('formula1-secret','formula1-secret-client-secret')
tenant_id=  dbutils.secrets.get('formula1-secret','formula1-secret-tenant-id')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databricksdlmg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databricksdlmg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databricksdlmg.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databricksdlmg.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databricksdlmg.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@databricksdlmg.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/raw",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl/raw"))

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dl/raw")

# COMMAND ----------

display(spark.read.csv("abfss://raw@databricksdlmg.dfs.core.windows.net/circuits.csv"))
