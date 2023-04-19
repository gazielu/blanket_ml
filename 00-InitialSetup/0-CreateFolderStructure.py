# Databricks notebook source
# MAGIC %md
# MAGIC ## Mounting Folders to DBFS

# COMMAND ----------

# MAGIC %run ../01-General/0-Mount_Indigo_Blob

# COMMAND ----------

# MAGIC %md
# MAGIC this is the root folder that we mount
# MAGIC <br>
# MAGIC MOUNTPOINT ="/mnt/Blankets_model"

# COMMAND ----------

MOUNTPOINT ="/mnt/Blankets_model"

# COMMAND ----------

MOUNTPOINT = "/mnt/Blankets_model"
SourcePath = MOUNTPOINT + "/azureml/Blankets_model"
RawPath = SourcePath + "/Dev/Raw"
ResearchPath = SourcePath + "/Dev/Research"
LandingPath = SourcePath + "/Dev/Landing"
LoadingPath = SourcePath + "/Dev/Loading"
display(dbutils.fs.ls(LandingPath+"/New_Era/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## list of relevant project folders

# COMMAND ----------

dbutils.fs.put(MOUNTPOINT + "/azureml/Blankets_model/Dev/Readme.txt", "Project Parent Folder")
dbutils.fs.put(MOUNTPOINT + "/azureml/Blankets_model/Dev/Research/", "contain all the reaserch file for the project as flat files (txt)")
dbutils.fs.put(MOUNTPOINT + "/azureml/Blankets_model/Dev/Source", " Contain just relevent source flat files for the project")
dbutils.fs.put(MOUNTPOINT + "/azureml/Blankets_model/Dev/Landing", " Parquet file load for Raw Zone ")
dbutils.fs.put(MOUNTPOINT + "/azureml/Blankets_model/Dev/Loading", " parquet file after cleasing landing files")
dbutils.fs.put(MOUNTPOINT + "/azureml/Blankets_model/Dev/certified-pit", " delta files ready for injestions")
dbutils.fs.put(MOUNTPOINT + "/azureml/Blankets_model/Dev/certified-pit/pandas/Readme.txt", "folder for external files")

#/dbfs/mnt/Blankets_model/azureml/Blankets_model/Dev/certified-pit/pandas/ce_Blankets_Production_lifespan_pandas


# COMMAND ----------

Parameters_run_level_dfp = spark.read.format("parquet").load(certified_pit + "/Blankets_Production_lifespan/ce_Blankets_Production_lifespan/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy files

# COMMAND ----------

dbutils.fs.cp(LandingPath + "/New_Era/New_era_details_Full.parquet.zip",  LandingPath + "/New_Era/New_era_details_zip/New_era_details_Full.parquet.zip")

#/New_Era_Run_Details/New_era_details.zip
#https://blanketpocmlstore.blob.core.windows.net/azureml/azureml/Blankets_model/Dev/Landing/New_Era/New_era_details_Full.parquet.zip

# COMMAND ----------

dbutils.fs.rm(MOUNTPOINT + "/azureml/Blankets_model/Dev/Landing/Production_run_gemini3_History",recurse=True)

# COMMAND ----------

display(dbutils.fs.ls(MOUNTPOINT + "/azureml/Blankets_model/Dev/Devs/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete non relevent folders

# COMMAND ----------

dbutils.fs.rm(MOUNTPOINT + "/azureml/Blankets_model/Dev/Devs/",recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## folders &  files functions
# MAGIC 
# MAGIC This is an example of how to list things you need to use the software and how to install them.
# MAGIC * remove folder
# MAGIC   ```python
# MAGIC   dbutils.fs.rm("/mnt/workshop/scratch/test/",recurse=True)
# MAGIC   ```
# MAGIC * display list files in folders
# MAGIC   ```python
# MAGIC   display(dbutils.fs.ls(MOUNTPOINT + "/azureml/Blankets_model/Dev/Devs/"))
# MAGIC   ```
# MAGIC * copy files 
# MAGIC   ```python
# MAGIC   dbutils.fs.cp(SourcePath + "/sample-text-file.txt",  MOUNTPOINT + "/azureml/Blankets_model/Dev/Devss/blanket_pareto.txt")
# MAGIC    ```