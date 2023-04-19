# Databricks notebook source
# MAGIC %md
# MAGIC # Load txt to parquet -  ingestion phase
# MAGIC 
# MAGIC 1) Dataframe schema definition<BR> 
# MAGIC 2) Read data from azure Row folder to landing zone<BR>
# MAGIC 3) Create Audit Log ??

# COMMAND ----------

# MAGIC %run ../01-General/0-Mount_Indigo_Blob

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. List files in relevent storage
# MAGIC 
# MAGIC This is an example of how to list things you need to use the software and how to install them.
# MAGIC * Azure blob storage indigo 
# MAGIC   ```sh
# MAGIC  display(dbutils.fs.ls(srcDataDirRoot))
# MAGIC   ```

# COMMAND ----------

MOUNTPOINT = "/mnt/Blankets_model"
SourcePath = MOUNTPOINT + "/azureml/Blankets_model"
RawPath = SourcePath + "/Dev/Raw"
ResearchPath = SourcePath + "/Dev/Research"
LandingPath = SourcePath + "/Dev/Landing"
LoadingPath = SourcePath + "/Dev/Loading"
display(dbutils.fs.ls(SourcePath))

# COMMAND ----------

dbutils.fs.cp(SourcePath + "/Production_run_rotem_sp.txt",  RawPath + "/Production_run_rotem.txt")

# COMMAND ----------

# Define source and destination directories
srcDataDirRoot = RawPath #Root dir for source data
destDataDirRoot = LandingPath #Root dir for consumable data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define Schema for the file that loaded

# COMMAND ----------

# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

# # # Space bi - sensor data
# ProductionRunSchema = StructType([
#   StructField("Product" , StringType() ,True),
#   StructField("Machine" , StringType() ,True),
#   StructField("FOLDER_PATH" , StringType() ,True),
#   StructField("Product_category" , StringType() ,True),
#   StructField("Product_eng_name" , StringType() ,True),
#   StructField("Series" , StringType() ,True),
#   StructField("Product_name_win" , StringType() ,True),
#   StructField("Size_Flag" , StringType() ,True),
#   StructField("Blanket_Type_Flag" , StringType() ,True),
#   StructField("Parameter_Name" , StringType() ,True),
#   StructField("Batch" , StringType() ,True),
#   StructField("SAMPLE_ID" , IntegerType() ,True),
#   StructField("SAMPLE_Date" , DateType() ,True),
#   StructField("SK_Sample_Date" , IntegerType() ,True),
#   StructField("Parameter_Critical_Flag" , StringType() ,True),
#   StructField("Is_Sample_Deleted_Flg" , StringType() ,True),
#   StructField("SAMPLE_Mean" , FloatType() ,True),
#   StructField("SAMPLE_stdev" , FloatType() ,True),
#   StructField("SAMPLE_Minimum" , FloatType() ,True),
#   StructField("SAMPLE_Maximum" , FloatType() ,True),
#   StructField("SAMPLE_Median" , FloatType() ,True),
#   StructField("Spec_target" , FloatType() ,True),
#   StructField("SAMPLE_Size" , IntegerType() ,True),
#   StructField("LSL" , FloatType() ,True),
#   StructField("USL" , FloatType() ,True),
#   StructField("SL_enabled" , StringType() ,True),
#   StructField("CH_ID" , IntegerType() ,True)
# ])


# # Space bi inc- sensor data 
ProductionRunSchema = StructType([
  StructField("Product" , StringType() ,True),
  StructField("Machine" , StringType() ,True),
  StructField("FOLDER_PATH" , StringType() ,True),
  StructField("Product_category" , StringType() ,True),
  StructField("Product_eng_name" , StringType() ,True),
  StructField("Series" , StringType() ,True),
  StructField("Product_name_win" , StringType() ,True),
  StructField("Size_Flag" , StringType() ,True),
  StructField("Blanket_Type_Flag" , StringType() ,True),
  StructField("Parameter_Name" , StringType() ,True),
  StructField("Batch" , StringType() ,True),
  StructField("SAMPLE_ID" , IntegerType() ,True),
  StructField("SAMPLE_Date" , DateType() ,True),
  StructField("SK_Sample_Date" , IntegerType() ,True),
  StructField("Parameter_Critical_Flag" , StringType() ,True),
  StructField("Is_Sample_Deleted_Flg" , StringType() ,True),
  StructField("SAMPLE_Mean" , FloatType() ,True),
  StructField("SAMPLE_stdev" , FloatType() ,True),
  StructField("SAMPLE_Minimum" , FloatType() ,True),
  StructField("SAMPLE_Maximum" , FloatType() ,True),
  StructField("SAMPLE_Median" , FloatType() ,True),
  StructField("Spec_target" , FloatType() ,True),
  StructField("SAMPLE_Size" , IntegerType() ,True),
  StructField("LSL" , FloatType() ,True),
  StructField("USL" , FloatType() ,True),
  StructField("SL_enabled" , StringType() ,True),
  StructField("CH_ID" , IntegerType() ,True),
  StructField("ETL_DATE" , TimestampType() ,True)
])

# #  #   Column              Non-Null Count  Dtype         
# # ---  ------              --------------  -----         
# #  0   SAMPLE_ID           1000 non-null   int32         
# #  1   SAMPLE_DATE FORMAT  1000 non-null   object        
# #  2   SAMPLE_DATE         1000 non-null   object        
# #  3   ETL_DATE            1000 non-null   datetime64[ns]

# # # # Space bi inc- sensor data 
#  ProductionRunSchemaTest = StructType([
# #   StructField("Product" , StringType() ,True),
# #   StructField("Machine" , StringType() ,True),
# #   StructField("FOLDER_PATH" , StringType() ,True),
# #   StructField("Product_category" , StringType() ,True),
# #   StructField("Product_eng_name" , StringType() ,True),
# #   StructField("Series" , StringType() ,True),
# #   StructField("Product_name_win" , StringType() ,True),
# #   StructField("Size_Flag" , StringType() ,True),
# #   StructField("Blanket_Type_Flag" , StringType() ,True),
# #   StructField("Parameter_Name" , StringType() ,True),
# #   StructField("Batch" , StringType() ,True),
#    StructField("SAMPLE_ID" , IntegerType() ,True),
#    StructField("SAMPLE_DATE FORMAT" , DateType() ,True),
#    StructField("SAMPLE_Date" , TimestampType() ,True),
# #   StructField("SK_Sample_Date" , IntegerType() ,True),
# #   StructField("Parameter_Critical_Flag" , StringType() ,True),
# #   StructField("Is_Sample_Deleted_Flg" , StringType() ,True),
# #   StructField("SAMPLE_Mean" , FloatType() ,True),
# #   StructField("SAMPLE_stdev" , FloatType() ,True),
# #   StructField("SAMPLE_Minimum" , FloatType() ,True),
# #   StructField("SAMPLE_Maximum" , FloatType() ,True),
# #   StructField("SAMPLE_Median" , FloatType() ,True),
# #   StructField("Spec_target" , FloatType() ,True),
# #   StructField("SAMPLE_Size" , IntegerType() ,True),
# #   StructField("LSL" , FloatType() ,True),
# #   StructField("USL" , FloatType() ,True),
# #   StructField("SL_enabled" , StringType() ,True),
# #   StructField("CH_ID" , IntegerType() ,True),
#    StructField("ETL_DATE" , TimestampType() ,True)
#  ])



# windigo production line
New_Era_Run_Details_Schema = StructType([
StructField("Run_Number" , StringType() ,True),
StructField("Blanket_Serial_Number" , StringType() ,True),  
StructField("Blanket_SEQ_NR" , StringType() ,True),  
StructField("Plant_Id" , IntegerType() ,True),
StructField("Quality_Status_Id" , IntegerType() ,True),
StructField("Body_ID" , IntegerType() ,True),
StructField("CSL_ID" , IntegerType() ,True),
StructField("BLK_Quality_Status_Name" , StringType() ,True),
StructField("BLK_Quality_Status_Flag" , StringType() ,True),
StructField("Blanket_Legacy_Part_Nr" , StringType() ,True),
StructField("Product_Engineering_Name" , StringType() ,True),
StructField("Source_System_Modified_DateTime" , TimestampType() ,True)
])

# # # Customer data - lifespan
# Blanket_lifespan_installed_base_Schema = StructType([
# StructField("Fact_PIP_IMPACT_RowID", IntegerType() ,True),
# StructField("Press_Serial_Number", IntegerType() ,True),
# StructField("BLANKETS_ID", StringType() ,True),
# StructField("Replacement_DateTime", TimestampType() ,True),
# StructField("End_User_Code", StringType() ,True),
# StructField("Domain", StringType() ,True),
# StructField("ROR", StringType() ,True),
# StructField("Consumable_Type", StringType() ,True),
# StructField("Optimized_Lifespan", IntegerType() ,True),
# StructField("Is_Last_Replacement", StringType() ,True),
# StructField("Is_Lifespan_Official", StringType() ,True),
# StructField("Consumable_Maturity", StringType() ,True),
# StructField("DOA_Count", IntegerType() ,True),
# StructField("DOP_Count", IntegerType() ,True),
# StructField("RowID", IntegerType() ,True),
# StructField("Changed_Date_Time", StringType() ,True),
# StructField("Replacement_Monthly_Date_Id", IntegerType() ,True),
# StructField("ETL_Date", StringType() ,True),
# StructField("Press_Classification", StringType() ,True),
# StructField("Lifespan_Guidelines", DoubleType() ,True),
# StructField("Click_Charge", StringType() ,True),
# StructField("Ownership", StringType() ,True),
# StructField("Product_Number", StringType() ,True),
# StructField("Description", StringType() ,True),
# StructField("Product_Group", StringType() ,True),
# StructField("Press_Group", StringType() ,True),
# StructField("Family_type", StringType() ,True),
# StructField("Series", StringType() ,True),
# StructField("Press_Segment", StringType() ,True),
# StructField("Current_SW_Version_ID", StringType() ,True),
# StructField("Customer_Name", StringType() ,True),
# StructField("Site_Region", StringType() ,True),
# StructField("Site_Sub_Region", StringType() ,True),
# StructField("Site_Country", StringType() ,True)
# ])



# COMMAND ----------


def loadReferenceData(srcDatasetName, srcDataFile, destDataDir, srcSchema, delimiter ):
  print("Dataset:  " + srcDatasetName)
  print(".......................................................")
  
  #Execute for idempotent runs
  print("....deleting destination directory - " + str(dbutils.fs.rm(destDataDir, recurse=True)))
  
  #Read source data
  refDF = (sqlContext.read.option("header", True)
                      .schema(srcSchema)
                      .option("delimiter",delimiter)
                      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
                      .csv(srcDataFile))
      
  #Write parquet output
  print(f"number of rows copyied:{refDF.count()}\n....reading source and saving as parquet")
  #refDF.coalesce(1).write.parquet(destDataDir)
  refDF.write.parquet(destDataDir,mode="overwrite")
  
  print("....done\n.................................")



# COMMAND ----------


def loadReferenceDataNoSchema(srcDatasetName, srcDataFile, destDataDir, srcSchema, delimiter ):
  print("Dataset:  " + srcDatasetName)
  print(".......................................................")
  
  #Execute for idempotent runs
  print("....deleting destination directory - " + str(dbutils.fs.rm(destDataDir, recurse=True)))
  
  #Read source data
  refDF = (sqlContext.read.option("header", True)
                      .option("inferSchema",True)
                      .option("delimiter",delimiter)
                   #   .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
                      .csv(srcDataFile))
      
  #Write parquet output
  print(f"number of rows copyied:{refDF.count()}\n....reading source and saving as parquet")
  #refDF.coalesce(1).write.parquet(destDataDir)
  refDF.write.parquet(destDataDir,mode="overwrite")
  
  print("....done\n.................................")

# COMMAND ----------

# # # Space bi inc- sensor data 
ProductionRunSchemaTest = StructType([
StructField("SAMPLE_ID" , IntegerType() ,True),
StructField("SAMPLE_DATE FORMAT" , DateType() ,True),
StructField("SAMPLE_DATE_CLEAN" , TimestampType() ,True),  
StructField("SAMPLE_Date" , TimestampType() ,True),
StructField("ETL_DATE" , TimestampType() ,True)
])



# COMMAND ----------

loadReferenceData("Production_run_hist",SourcePath + '/test.csv',SourcePath + '/test',ProductionRunSchema,',')

# COMMAND ----------

!pip install pyodbc

# COMMAND ----------

import pyodbc
# Create connection string to GABI-P for PIP APL data
conn_GABI = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                        "Server=gvs72069.inc.hpicorp.net,2048;"
                        "Database=GABI-P;"
                        "trusted_connection=yes")


# COMMAND ----------

Parameters_run_level_dfspark.savaAsTable("Test")# = spark.read.format("parquet").load(SourcePath + '/test')

# COMMAND ----------

Parameters_run_level_dfspark.printSchema()


# COMMAND ----------

Parameters_run_level_dfp = spark.read.format("parquet").load(SourcePath + '/test').toPandas()


# COMMAND ----------

import pandas as pd

# COMMAND ----------

Parameters_run_level_dfp.info()

# COMMAND ----------

Parameters_run_level_dfp['SAMPLE_Date'] =  pd.to_datetime(Parameters_run_level_dfp['SAMPLE_Date'], infer_datetime_format=True)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

Parameters_run_level_dfp.head()

# COMMAND ----------

loadReferenceDataNoSchema("Production_run_hist",srcDataDirRoot + '/Production_run_rotem.txt',destDataDirRoot + '/Production_run/Production_run_rotem',ProductionRunSchema,'\t')

# COMMAND ----------

Parameters_run_level_dfp = spark.read.format("parquet").load(LandingPath + "/Production_run/Production_run_rotem/").toPandas()

# ld_blanket_lifespan_run_extended.write.format("parquet").option("path",LoadingPath + "/Blanket_lifespan/ld_blanket_lifespan_run_extended").mode("overwrite").saveAsTable("ld_blanket_lifespan_run_extended")

# COMMAND ----------

Parameters_run_level_dfp.head()

# COMMAND ----------

Parameters_run_level_dfp_gemini = spark.read.format("parquet").load(LandingPath + "/Production_run/Production_run_gemini3_hist/").toPandas()

# COMMAND ----------

Parameters_run_level_dfp_gemini.head()

# COMMAND ----------

loadReferenceDataNoSchema("Production_run_hist",srcDataDirRoot + '/Production_run_rotem.txt',destDataDirRoot + '/Production_run/Production_run_rotem',ProductionRunSchema,'\t')

# COMMAND ----------

loadReferenceData("New_Era_hist",srcDataDirRoot + '/New_Era_Run_Details.txt',destDataDirRoot + '/New_Era/New_Era_hist',New_Era_Run_Details_Schema,'\t')
loadReferenceData("Production_run_hist",srcDataDirRoot + '/Production_run_gemini3.txt',destDataDirRoot + '/Production_run/Production_run_gemini3_hist',ProductionRunSchema,'\t')
loadReferenceData("Blanket_lifespan_hist",srcDataDirRoot + '/Blanket_lifespan_installed_base_guidlines.txt',destDataDirRoot + '/Blanket_lifespan/Blanket_lifespan_hist',Blanket_lifespan_installed_base_Schema,'\t')
loadReferenceData("Production_run_hist",srcDataDirRoot + '/Production_run_rotem.txt',destDataDirRoot + '/Production_run/Production_run_rotem',ProductionRunSchema,'\t')
loadReferenceData("Production_run_hist",srcDataDirRoot + '/Production_run_iris_plus.txt',destDataDirRoot + '/Production_run/Production_run_iris_plus',ProductionRunSchema,'\t')
loadReferenceData("Production_run_hist",srcDataDirRoot + '/Production_run_Timna2.txt',destDataDirRoot + '/Production_run/Production_run_Timna2',ProductionRunSchema,'\t')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 4.Create pyspark Dataframe

# COMMAND ----------

# New_Era_Run_Details.coalesce(2).write.parquet(LandingPath + "/New_Era_Run_Summary",mode="overwrite")
# Production_run_gemini3.coalesce(2).write.parquet(LandingPath + "/Production_run_gemini",mode="overwrite")
# Blanket_lifespan_installed_base.coalesce(2).write.parquet(LandingPath + "/Blanket_lifespan/Blanket_lifespan_hist",mode="overwrite").toPandas.head()

# COMMAND ----------

parqDF = spark.read.parquet(LandingPath + "/Production_run/Production_run_gemini3_hist")
parqDF.createOrReplaceTempView("Table2")
##spark.sql("select * from Table2").show().take(5)

# COMMAND ----------

df.show().take(5)

# COMMAND ----------



loadReferenceData("Production_run_hist_inc",srcDataDirRoot + '/Production_run_gemini3_ETL_Inc.txt',destDataDirRoot + '/Production_run/Production_run_gemini3_Inc',ProductionRunSchema,'\t')

# COMMAND ----------

parqDF = spark.read.parquet(LandingPath + "/Production_run/Production_run_gemini3_Inc")
parqDF.createOrReplaceTempView("Production_run_gemini3_Inc")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Production_run_gemini3_Inc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from blankets_db.lz_production_run_gemini_hist

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables from blankets_db

# COMMAND ----------

parqDF.select(min('ETL_DATE')).collect()

# COMMAND ----------

parqDF.agg({'ETL_DATE': 'min'}).show()

# COMMAND ----------

from pyspark.sql import functions

# COMMAND ----------

parqDF.groupby('Machine').agg(functions.min('ETL_DATE') \
                             .functions.max('ETL_DATE')) \
                             .show()




# COMMAND ----------

