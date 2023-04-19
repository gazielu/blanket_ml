# Databricks notebook source
# MAGIC %run ../01-General/0-Mount_Indigo_Blob

# COMMAND ----------

MOUNTPOINT = "/mnt/Blankets_model"
SourcePath = MOUNTPOINT + "/azureml/Blankets_model"
RawPath = SourcePath + "/Dev/Raw"
ResearchPath = SourcePath + "/Dev/Research"
LandingPath = SourcePath + "/Dev/Landing"
display(dbutils.fs.ls(LandingPath))

# COMMAND ----------

  # local file uploaded 

New_Era_Run_Details = spark.read.format("parquet").load("dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/New_Era/New_era_details_Full.parquet/")

production_run_level = spark.read.format("parquet").load("dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/Production_run/ProductionRun_Full_YM/ProductionRun_Full_YM.parquet/")

Blanket_lifespan_hist = spark.read.format("parquet").load("dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/Blanket_lifespan/Blanket_lifespan_hist_PM.parquet/")

#New_era_details_dfs.coalesce(2).write.parquet(LandingPath + "/New_Era/New_Era_Run_Summary_N",mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create External table

# COMMAND ----------

# MAGIC     
# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS BLANKETS_DB.lz_Production_run_gemini_hist;
# MAGIC CREATE TABLE blankets_db.lz_production_run_gemini_hist (
# MAGIC   Product STRING,
# MAGIC   Machine STRING,
# MAGIC   FOLDER_PATH STRING,
# MAGIC   Product_category STRING,
# MAGIC   Product_eng_name STRING,
# MAGIC   Series STRING,
# MAGIC   Product_name_win STRING,
# MAGIC   Size_Flag STRING,
# MAGIC   Parameter_Name STRING,
# MAGIC   Batch STRING,
# MAGIC   SAMPLE_ID FLOAT,
# MAGIC   SAMPLE_Date TIMESTAMP,
# MAGIC   SK_Sample_Date INT,
# MAGIC   Parameter_Critical_Flag STRING,
# MAGIC   Is_Sample_Deleted_Flg STRING,
# MAGIC   SAMPLE_Mean FLOAT,
# MAGIC   SAMPLE_stdev FLOAT,
# MAGIC   SAMPLE_Minimum FLOAT,
# MAGIC   SAMPLE_Maximum FLOAT,
# MAGIC   SAMPLE_Median FLOAT,
# MAGIC   Spec_target FLOAT,
# MAGIC   SAMPLE_Size FLOAT,
# MAGIC   LSL FLOAT,
# MAGIC   USL FLOAT,
# MAGIC   SL_enabled STRING,
# MAGIC   CH_ID FLOAT,
# MAGIC   ETL_DATE TIMESTAMP,
# MAGIC   YearMonth INT
# MAGIC   )
# MAGIC USING PARQUET
# MAGIC --LOCATION 'dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/Production_run/Production_run_gemini3_hist'; -- Old with text file
# MAGIC --LOCATION 'dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/Production_run/Production_run_N'; -- Old with text file
# MAGIC LOCATION 'dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/Production_run/ProductionRun_Full_YM/ProductionRun_Full_YM.parquet/'; -- Old with text file
# MAGIC ANALYZE TABLE blankets_db.lz_production_run_gemini_hist COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql  
# MAGIC USE BLANKETS_DB;
# MAGIC DROP TABLE IF EXISTS blankets_db.lz_new_era_hist;
# MAGIC CREATE TABLE IF NOT EXISTS blankets_db.lz_new_era_hist (
# MAGIC   Run_Number STRING,
# MAGIC   Blanket_Serial_Number STRING,
# MAGIC   Blanket_SEQ_NR STRING,
# MAGIC   Plant_Id INT,
# MAGIC   Quality_Status_Id INT,
# MAGIC   Body_ID INT,
# MAGIC   CSL_ID INT,
# MAGIC   BLK_Quality_Status_Name STRING,
# MAGIC   BLK_Quality_Status_Flag STRING,
# MAGIC   Blanket_Legacy_Part_Nr STRING,
# MAGIC   Product_Engineering_Name STRING,
# MAGIC   Source_System_Modified_DateTime TIMESTAMP)
# MAGIC USING parquet
# MAGIC --LOCATION 'dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/New_Era/New_Era_hist'; -- read from text old
# MAGIC LOCATION 'dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/New_Era/New_era_details_Full.parquet/';
# MAGIC ANALYZE TABLE lz_new_era_hist COMPUTE STATISTICS;
# MAGIC 
# MAGIC -- azureml/Blankets_model/Dev/Landing/New_Era/New_era_details_Full.parquet/part-00000-8636c17c-b9e7-4d62-9580-87cff73fb3fe-c000.snappy.parquet

# COMMAND ----------

# MAGIC %sql  
# MAGIC USE BLANKETS_DB;
# MAGIC DROP TABLE IF EXISTS blankets_db.lz_blanket_lifespan_hist;
# MAGIC CREATE TABLE IF NOT EXISTS blankets_db.lz_blanket_lifespan_hist (
# MAGIC     Blanket_Impact_RowID STRING,
# MAGIC   Press_Serial_Number STRING,
# MAGIC   BLANKETS_ID STRING,
# MAGIC   Replacement_DateTime TIMESTAMP,
# MAGIC   End_User_Code STRING,
# MAGIC   Domain STRING,
# MAGIC   ROR STRING,
# MAGIC   Consumable_Type STRING,
# MAGIC   Optimized_Lifespan INT,
# MAGIC   Is_Last_Replacement STRING,
# MAGIC   Is_Lifespan_Official STRING,
# MAGIC   Consumable_Maturity STRING,
# MAGIC   DOA_Count BIGINT,
# MAGIC   DOP_Count BIGINT,
# MAGIC   RowID BIGINT,
# MAGIC   Changed_Date_Time TIMESTAMP,
# MAGIC   Replacement_Monthly_Date_Id BIGINT,
# MAGIC   ETL_Date TIMESTAMP,
# MAGIC   Click_Charge STRING,
# MAGIC   Ownership STRING,
# MAGIC   Product_Number STRING,
# MAGIC   Description STRING,
# MAGIC   Product_Group STRING,
# MAGIC   Press_Group STRING,
# MAGIC   Family_type STRING,
# MAGIC   Series STRING,
# MAGIC   Press_Segment STRING,
# MAGIC   Current_SW_Version_ID STRING,
# MAGIC   Customer_Name STRING,
# MAGIC   Site_Region STRING,
# MAGIC   Site_Sub_Region STRING,
# MAGIC   Site_Country STRING)
# MAGIC USING PARQUET
# MAGIC --LOCATION 'dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/Blanket_lifespan/Blanket_lifespan_hist';-- old load from text (sql manual)
# MAGIC LOCATION 'dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/Blanket_lifespan/Blanket_lifespan_hist_PM.parquet/';
# MAGIC ANALYZE TABLE blankets_db.lz_blanket_lifespan_hist COMPUTE STATISTICS;