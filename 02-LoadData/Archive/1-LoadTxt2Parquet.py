# Databricks notebook source
display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Load txt to parquet -  ingestion phase
# MAGIC 
# MAGIC 1) Dataframe schema definition<BR> 
# MAGIC 2) Read data from azure Row folder to landing zone<BR>
# MAGIC 3) Create Audit Log ??

# COMMAND ----------

# MAGIC %run ../01-General/0-Mount_Indigo_Blob

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
# MAGIC ## 1. List files in relevent storage
# MAGIC 
# MAGIC This is an example of how to list things you need to use the software and how to install them.
# MAGIC * Azure blob storage indigo 
# MAGIC   ```sh
# MAGIC  display(dbutils.fs.ls(srcDataDirRoot))
# MAGIC   ```

# COMMAND ----------

dbutils.fs.put("dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/New_Era/Full/Readme.txt", "Project Parent Folder")
dbutils.fs.cp("dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/New_Era/part-00000-ec7ec021-c4b6-4a10-9209-7d3259237051-c000.snappy.parquet",  "dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/New_Era/Full/part-00000-ec7ec021-c4b6-4a10-9209-7d3259237051-c000.snappy.parquet")

# COMMAND ----------

#Parameters_run_level_dfp2 = spark.read.format("parquet").load("dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/New_Era/Full/*")

#azureml/Blankets_model/Dev/Landing/New_Era/New_era_details_Full.parquet/

# COMMAND ----------

# local file uploaded 

New_Era_Run_Details = spark.read.format("parquet").load("dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/New_Era/New_era_details_Full.parquet/")

Parameters_run_level = spark.read.format("parquet").load("dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/Production_run/ProductionRun_Full_YM/ProductionRun_Full_YM.parquet/")

Blanket_lifespan_hist = spark.read.format("parquet").load("dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/Blanket_lifespan/Blanket_lifespan_hist_PM.parquet/")


# COMMAND ----------

#Parameters_run_level.count()

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

# # Space bi - sensor data
ProductionRunSchema = StructType([
  StructField("Product" , StringType() ,True),
  StructField("Machine" , StringType() ,True),
  StructField("FOLDER_PATH" , StringType() ,True),
  StructField("Product_category" , StringType() ,True),
  StructField("Product_eng_name" , StringType() ,True),
  StructField("Series" , StringType() ,True),
  StructField("Product_name_win" , StringType() ,True),
  StructField("Size_Flag" , StringType() ,True),
  #StructField("Blanket_Type_Flag" , StringType() ,True),
  StructField("Parameter_Name" , StringType() ,True),
  StructField("Batch" , StringType() ,True),
  StructField("SAMPLE_ID" , FloatType() ,True),
  StructField("SAMPLE_Date" , TimestampType() ,True),
  StructField("SK_Sample_Date" , IntegerType() ,True),
  StructField("Parameter_Critical_Flag" , StringType() ,True),
  StructField("Is_Sample_Deleted_Flg" , StringType() ,True),
  StructField("SAMPLE_Mean" , FloatType() ,True),
  StructField("SAMPLE_stdev" , FloatType() ,True),
  StructField("SAMPLE_Minimum" , FloatType() ,True),
  StructField("SAMPLE_Maximum" , FloatType() ,True),
  StructField("SAMPLE_Median" , FloatType() ,True),
  StructField("Spec_target" , FloatType() ,True),
  StructField("SAMPLE_Size" , FloatType() ,True),
  StructField("LSL" , FloatType() ,True),
  StructField("USL" , FloatType() ,True),
  StructField("SL_enabled" , StringType() ,True),
  StructField("CH_ID" , FloatType() ,True),
  StructField("ETL_DATE" , TimestampType() ,True)
])


# # # Space bi inc- sensor data 
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
#   StructField("CH_ID" , IntegerType() ,True),
#   StructField("ETL_DATE" , TimestampType() ,True)
# ])


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

# # Customer data - lifespan
Blanket_lifespan_installed_base_Schema = StructType([
StructField("Fact_PIP_IMPACT_RowID", IntegerType() ,True),
StructField("Press_Serial_Number", IntegerType() ,True),
StructField("BLANKETS_ID", StringType() ,True),
StructField("Replacement_DateTime", TimestampType() ,True),
StructField("End_User_Code", StringType() ,True),
StructField("Domain", StringType() ,True),
StructField("ROR", StringType() ,True),
StructField("Consumable_Type", StringType() ,True),
StructField("Optimized_Lifespan", IntegerType() ,True),
StructField("Is_Last_Replacement", StringType() ,True),
StructField("Is_Lifespan_Official", StringType() ,True),
StructField("Consumable_Maturity", StringType() ,True),
StructField("DOA_Count", IntegerType() ,True),
StructField("DOP_Count", IntegerType() ,True),
StructField("RowID", IntegerType() ,True),
StructField("Changed_Date_Time", StringType() ,True),
StructField("Replacement_Monthly_Date_Id", IntegerType() ,True),
StructField("ETL_Date", StringType() ,True),
StructField("Press_Classification", StringType() ,True),
StructField("Lifespan_Guidelines", DoubleType() ,True),
StructField("Click_Charge", StringType() ,True),
StructField("Ownership", StringType() ,True),
StructField("Product_Number", StringType() ,True),
StructField("Description", StringType() ,True),
StructField("Product_Group", StringType() ,True),
StructField("Press_Group", StringType() ,True),
StructField("Family_type", StringType() ,True),
StructField("Series", StringType() ,True),
StructField("Press_Segment", StringType() ,True),
StructField("Current_SW_Version_ID", StringType() ,True),
StructField("Customer_Name", StringType() ,True),
StructField("Site_Region", StringType() ,True),
StructField("Site_Sub_Region", StringType() ,True),
StructField("Site_Country", StringType() ,True)
])



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

loadReferenceData("Blanket_lifespan_hist",srcDataDirRoot + '/Blanket_lifespan_installed_base_guidlines.txt',destDataDirRoot + '/Blanket_lifespan/Blanket_lifespan_hist',Blanket_lifespan_installed_base_Schema,'\t')

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

New_Era_Run_Details.coalesce(2).write.parquet(LandingPath + "/New_Era_Run_Summary",mode="overwrite")
# Production_run_gemini3.coalesce(2).write.parquet(LandingPath + "/Production_run_gemini",mode="overwrite")
Blanket_lifespan_installed_base.coalesce(2).write.parquet(LandingPath + "/Blanket_lifespan/Blanket_lifespan_hist",mode="overwrite").toPandas.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### new load from FULL ETL LOAD

# COMMAND ----------

#### load New Files
New_era_details_dfs = spark.read.format("parquet").schema(New_Era_Run_Details_Schema).load(LandingPath + "/New_Era/part-00000-239e0339-1993-46f0-aeec-5992c7c373ce-c000.snappy.parquet")
New_era_details_dfs.coalesce(2).write.parquet(LandingPath + "/New_Era/New_Era_Run_Summary_N",mode="overwrite")

# COMMAND ----------

New_era_details_dfs.printSchema()


# COMMAND ----------


# windigo production line
New_Era_Run_Details_Schema = StructType([
StructField("Run_Number" , StringType() ,True),
StructField("Blanket_Serial_Number" , StringType() ,True),  
StructField("Blanket_SEQ_NR" , StringType() ,True),  
StructField("Plant_Id" , IntegerType() ,True),
StructField("Quality_Status_Id" , IntegerType() ,True),
StructField("Body_ID" , StringType() ,True),
StructField("CSL_ID" , StringType() ,True),
StructField("BLK_Quality_Status_Name" , StringType() ,True),
StructField("BLK_Quality_Status_Flag" , IntegerType() ,True),
StructField("Blanket_Legacy_Part_Nr" , StringType() ,True),
StructField("Product_Engineering_Name" , StringType() ,True),
StructField("Source_System_Modified_DateTime" , TimestampType() ,True)
])

# COMMAND ----------

#### load Blankets_files

Blanket_lifespan_hist_N = spark.read.format("parquet").schema(Blanket_lifespan_installed_base_Schema).load(LandingPath + "/Blanket_lifespan/part-00000-8e255d9a-255b-4f9b-a72b-f8fbcdcc4922-c000.snappy.parquet")
Blanket_lifespan_hist_N.coalesce(2).write.parquet(LandingPath + "/Blanket_lifespan/Blanket_lifespan_hist_N",mode="overwrite")


# COMMAND ----------

#### load Blankets_files

Production_run_N = spark.read.format("parquet").schema(ProductionRunSchema).load(LandingPath + "/Production_run/part-00000-bcf7a607-60d6-4264-bd50-457f19d9b920-c000.snappy.parquet")
Production_run_N.coalesce(2).write.parquet(LandingPath + "/Production_run/Production_run_N",mode="overwrite")


# https://blanketpocmlstore.blob.core.windows.net/azureml/azureml/Blankets_model/Dev/Landing/Production_run/part-00000-bcf7a607-60d6-4264-bd50-457f19d9b920-c000.snappy.parquet

#https://blanketpocmlstore.blob.core.windows.net/azureml/azureml/Blankets_model/Dev/Landing/Production_run/part-00000-621af9fc-533f-4473-a7ff-897b6ff08725-c000.snappy.parquet

# COMMAND ----------

Production_run_N.printSchema()


# COMMAND ----------

