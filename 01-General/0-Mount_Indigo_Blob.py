# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC MOUNTPOINT ="/mnt/Blankets_model"
# MAGIC #BLANKETS_MOUNTPOINT = "/mnt/azureml/azureml/Blankets_model"
# MAGIC 
# MAGIC if MOUNTPOINT in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
# MAGIC   dbutils.fs.unmount(MOUNTPOINT)

# COMMAND ----------

dbutils.fs.mount(
  source = 'wasbs://azureml@blanketpocmlstore.blob.core.windows.net',
  mount_point = '/mnt/Blankets_model',
  extra_configs = {'fs.azure.account.key.blanketpocmlstore.blob.core.windows.net':'0UvDsCSpMhJYLnbe9IErqC1nyKP7qPn9QKoTiS8N3rLSGXXbVabZ+6/sNLUA2grnzEj0Nd1cJwL7+AStx9Ogiw=='})




# COMMAND ----------

MOUNTPOINT ="/mnt/Blankets_model"

# COMMAND ----------

SourcePath = MOUNTPOINT + "/azureml/Blankets_model"
RawPath = SourcePath + "/Dev/Raw" 
ResearchPath = SourcePath + "/Dev/Research"
LandingPath = SourcePath + "/Dev/Landing"
LoadingPath = SourcePath + "/Dev/Loading"
certified_pit = SourcePath + "/Dev/certified-pit"