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

RawPath = SourcePath + "/Dev/Raw"
ResearchPath = SourcePath + "/Dev/Research"
LandingPath = SourcePath + "/Dev/Landing"
display(dbutils.fs.ls(LandingPath))

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
  StructField("Wide / Narrow Flag" , StringType() ,True),
  StructField("SF / Web Flag" , StringType() ,True),
  StructField("Parameter_Name" , StringType() ,True),
  StructField("Batch" , StringType() ,True),
  StructField("SAMPLE_ID" , IntegerType() ,True),
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
  StructField("SAMPLE_Size" , IntegerType() ,True),
  StructField("LSL" , FloatType() ,True),
  StructField("USL" , FloatType() ,True),
  StructField("SL_enabled" , StringType() ,True),
  StructField("CH_ID" , IntegerType() ,True)
])

# windigo production line
New_Era_Run_Details_Schema = StructType([
StructField("RUN" , StringType() ,True),
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
                      .csv(srcDataFile).limit(10))
      
  #Write parquet output
  print(f"number of rows copyied:{refDF.count()}\n....reading source and saving as parquet")
  #refDF.coalesce(1).write.parquet(destDataDir)
  refDF.coalesce(2).write.parquet(destDataDir,mode="overwrite")
  
  print("....done\n.................................")



# COMMAND ----------

loadReferenceData("New_Era_hist",srcDataDirRoot + '/New_Era_Run_Details.txt',destDataDirRoot + '/New_Era/New_Era_hist',New_Era_Run_Details_Schema,'\t')
loadReferenceData("Production_run_gemini3_hist",srcDataDirRoot + '/Production_run_gemini3.txt',destDataDirRoot + '/Production_run/Production_run_gemini3_hist',ProductionRunSchema,'\t')
loadReferenceData("Blanket_lifespan_hist",srcDataDirRoot + '/Blanket_lifespan_installed_base_guidlines.txt',destDataDirRoot + '/Blanket_lifespan/Blanket_lifespan_hist',Blanket_lifespan_installed_base_Schema,'\t')

# COMMAND ----------

dbutils.fs.cp(SourcePath + "/New_Era_Run_Details.txt",  RawPath + "/New_Era_Run_Details.txt")

# COMMAND ----------

dbutils.fs.head(RawPath + "/New_Era_Run_Details.txt",1000)

# COMMAND ----------



parDF1=spark.read.parquet(destDataDirRoot + '/New_Era_Run_Details_History')


# COMMAND ----------

parDF1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Cleasing Txt file

# COMMAND ----------

# windigo
New_Era_Run_Details_File = RawPath + "/New_Era_Run_Details.txt"
New_Era_Run_Details = (spark.read.options(header='True', delimiter='\t') \
              #.option("inferSchema", "true") \
              .schema(New_Era_Run_Details_Schema) \
              .option("timestampFormat", "YYYY-MM-DDThh:mm:ssTZD") 
              .csv(New_Era_Run_Details_File))

# COMMAND ----------

New_Era_Run_Details.createOrReplaceTempView("New_Era_Run_Details")

# COMMAND ----------

New_Era_Run_Details.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Creating Dataframes

# COMMAND ----------



  # # production run load to dataframe - Space
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
Production_run_gemini3File = RawPath + "/Production_run_gemini3.txt"
Production_run_gemini3 = (spark.read.options(header='True', delimiter='\t') \
              .schema(ProductionRunSchema) \
              .option("timestampFormat", "yy/mm/dd HH:mm:ss") 
              .csv(Production_run_gemini3File)) 
# windigo
New_Era_Run_Details_File = RawPath + "/New_Era_Run_Details.txt"
New_Era_Run_Details = (spark.read.options(header='True', delimiter='\t') \
              #.option("inferSchema", "true") \
              .schema(New_Era_Run_Details_Schema) \
              .option("timestampFormat", "YYYY-MM-DDThh:mm:ssTZD") 
              .csv(New_Era_Run_Details_File)) 

# Blankets_lifespan
Blanket_lifespan_installed_base_FilePath = RawPath + "/Blanket_lifespan_installed_base_guidlines.txt"
Blanket_lifespan_installed_base = (spark.read.options(header='True', delimiter='\t') \
              #.option("inferSchema", "true") \
              .schema(Blanket_lifespan_installed_base_Schema) \
              #.option("timestampFormat", "yyyy/MM/dd HH:mm:ss") 
              .csv(Blanket_lifespan_installed_base_FilePath)
              # .limit(20)
              ) 


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 4.Create pyspark Dataframe

# COMMAND ----------

New_Era_Run_Details.coalesce(2).write.parquet(LandingPath + "/New_Era_Run_Summary",mode="overwrite")
Production_run_gemini3.coalesce(2).write.parquet(LandingPath + "/Production_run_gemini",mode="overwrite")
Blanket_lifespan_installed_base.coalesce(2).write.parquet(LandingPath + "/Blanket_lifespan_installed_base",mode="overwrite")