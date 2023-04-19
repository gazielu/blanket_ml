# Databricks notebook source
# MAGIC %md
# MAGIC # Tasks
# MAGIC 1. need to filter DOA & DOP

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql import SparkSession
# MAGIC spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
# MAGIC 
# MAGIC configurations = spark.sparkContext.getConf().getAll()
# MAGIC for item in configurations: print(item)

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial Notebook Set up

# COMMAND ----------

import pandas as pd
import pyspark.pandas as ps

# COMMAND ----------

# MAGIC %run ../01-General/0-Mount_Indigo_Blob

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE DATABASE blankets_db 

# COMMAND ----------

MOUNTPOINT = "/mnt/Blankets_model"
SourcePath = MOUNTPOINT + "/azureml/Blankets_model"
RawPath = SourcePath + "/Dev/Raw"
ResearchPath = SourcePath + "/Dev/Research"
LandingPath = SourcePath + "/Dev/Landing"
LoadingPath = SourcePath + "/Dev/Loading"
certified_pit = SourcePath + "/Dev/certified-pit"
display(dbutils.fs.ls(LoadingPath))

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transpose parameter table + Create Paramter Pivot
# MAGIC <br> create generic table by reduce spabe bi (parameters data) columns (only batch,paramter name, value) for pivoting table to wide table and then attached to the customer impression data 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  * FROM blankets_db.lz_production_run_gemini_hist limit 10

# COMMAND ----------


sql = '''
  (SELECT   Batch,Parameter_Name || '_Mean' AS Parameter,SAMPLE_Mean AS Value FROM blankets_db.lz_production_run_gemini_hist)
  UNION
  (SELECT   Batch,Parameter_Name || '_stdev' AS Parameter,SAMPLE_stdev AS Value FROM blankets_db.lz_production_run_gemini_hist)'''
# capture stats in dataframe 
parameters_run_level_Long_df = spark.sql(sql)
# write table to parquet
#parameters_run_level_Long_df.write.parquet(LoadingPath + "/parameters_run/ld_parameters_run_level_Long/",mode="overwrite")


# COMMAND ----------

# MAGIC %sql select * from blankets_db.lz_production_run_gemini_hist limit 10

# COMMAND ----------

parameters_run_level_Long_df.show()

# COMMAND ----------

# MAGIC %python
# MAGIC # use pandas for 
# MAGIC parameters_run_level_pivot_df = parameters_run_level_Long_df.groupBy('Batch').pivot('Parameter').avg('Value')

# COMMAND ----------


parameters_run_level_pivot_dfp = parameters_run_level_pivot_df.toPandas()
df_columns_series = parameters_run_level_pivot_dfp.columns
Columns_less_the_two_unique_values = list(df_columns_series[parameters_run_level_pivot_dfp.nunique()<=1])


# COMMAND ----------

Columns_less_the_two_unique_values =['AIR_DR IN_TEMP_Mean', 'AIR_DR IN_TEMP_stdev', 'AIR_DR IN_VELOCITY_Mean', 'AIR_DR IN_VELOCITY_stdev', 'AIR_DR OUT_TEMP_Mean', 'AIR_DR OUT_TEMP_stdev', 'AIR_DR OUT_VELOCITY_Mean', 'AIR_DR OUT_VELOCITY_stdev', 'AIR_UV_LMP COOLING IN_TEMP_Mean', 'AIR_UV_LMP COOLING IN_TEMP_stdev', 'AIR_UV_LMP COOLING IN_VELOCITY_Mean', 'AIR_UV_LMP COOLING IN_VELOCITY_stdev', 'AIR_UV_LMP COOLING OUT_TEMP_Mean', 'AIR_UV_LMP COOLING OUT_TEMP_stdev', 'AIR_UV_LMP COOLING OUT_VELOCITY_Mean', 'AIR_UV_LMP COOLING OUT_VELOCITY_stdev', 'AIR_UV_QRZ COOLING_TEMP_Mean', 'AIR_UV_QRZ COOLING_TEMP_stdev', 'AIR_UV_QRZ COOLING_VELOCITY_Mean', 'AIR_UV_QRZ COOLING_VELOCITY_stdev', 'AIR_UV_SOLV OUT_TEMP_Mean', 'AIR_UV_SOLV OUT_TEMP_stdev', 'AIR_UV_SOLV OUT_VELOCITY_Mean', 'AIR_UV_SOLV OUT_VELOCITY_stdev', 'CSL_HP_SEAM_COUNTS_Mean', 'CSL_HP_SEAM_COUNTS_stdev', 'CTR_1_PUMP_SPEED_FDBK_Mean', 'CTR_1_PUMP_SPEED_FDBK_stdev', 'CTR_2_GRVR_ROLL_TORQUE_SP_Mean', 'CTR_2_GRVR_ROLL_TORQUE_SP_stdev', 'CTR_3_GRVR_ROLL_TORQUE_SP_Mean', 'CTR_3_GRVR_ROLL_TORQUE_SP_stdev', 'CTR_4_GRVR_ROLL_TORQUE_SP_Mean', 'CTR_4_GRVR_ROLL_TORQUE_SP_stdev', 'CTR_4_GRVR_SPEED_FDBK_Mean', 'CTR_4_GRVR_SPEED_FDBK_stdev', 'CTR_4_PUMP_SPEED_FDBK_Mean', 'CTR_4_PUMP_SPEED_FDBK_stdev', 'CTR_4_XFR_ROLL_TORQUE_SP_stdev', 'DR_EXHAUST_SPEED_FDBK_Mean', 'DR_EXHAUST_SPEED_FDBK_stdev', 'DR_SUPPLY_SPEED_FB_Mean', 'DR_SUPPLY_SPEED_FB_stdev', 'DR_VELOCITY_FB_Mean', 'DR_VELOCITY_FB_stdev', 'ENC_EXHAUST_SPEED_FDBK_Mean', 'ENC_EXHAUST_SPEED_FDBK_stdev', 'ITM-R_EXHAUST_SPEED_FDBK_Mean', 'ITM-R_EXHAUST_SPEED_FDBK_stdev', 'ITM-R_HOT_OIL_TEMP_FB_Mean', 'ITM-R_HOT_OIL_TEMP_FB_stdev', 'UV_LAMP_POWER_Mean', 'UV_LAMP_POWER_stdev', 'UV_LAMP_TEMP_Mean', 'UV_LAMP_TEMP_stdev', 'UV_LAMP_VOLTS_Mean', 'UV_LAMP_VOLTS_stdev']

# COMMAND ----------

parameters_run_level_pivot_temp = spark.createDataFrame(parameters_run_level_pivot_dfp.drop(Columns_less_the_two_unique_values,axis=1))
print(f'total columns droped:{len(Columns_less_the_two_unique_values)}\nlist of columns Droped by unique values: {Columns_less_the_two_unique_values}\ndata frame structure after droping  {parameters_run_level_pivot_temp.count(),len(parameters_run_level_pivot_temp.columns)}')

# COMMAND ----------

parameters_run_level_pivot = parameters_run_level_pivot_temp.withColumnRenamed("ITM_R ROLL_TORQUE_FDBK_Mean","ITM_R_ROLL_TORQUE_FDBK_Mean") \
.withColumnRenamed("ITM_R ROLL_TORQUE_FDBK_stdev","ITM_R_ROLL_TORQUE_FDBK_stdev") \
.withColumnRenamed("SCRAP_WNDR_3_TORQUE(name_edited)_Mean","SCRAP_WNDR_3_TORQUE_Mean") \
.withColumnRenamed("SCRAP_WNDR_3_TORQUE(name_edited)_stdev","SCRAP_WNDR_3_TORQUE_stdev") \
.withColumnRenamed("SCRAP_WNDR_3_TORQUE_FDBK(name_edited)_Mean","SCRAP_WNDR_3_TORQUE_FDBK_Mean") \
.withColumnRenamed("SCRAP_WNDR_3_TORQUE_FDBK(name_edited)_stdev","SCRAP_WNDR_3_TORQUE_FDBK_stdev") 

# COMMAND ----------

#parameters_run_level_pivot_df.write.parquet(LoadingPath + "/parameters_run/ld_parameters_run_level_pivot",mode="overwrite")
parameters_run_level_pivot.write.format("delta").option("path",LoadingPath + "/parameters_run/ld_parameters_run_level_pivot/").mode("overwrite").saveAsTable("ld_parameters_run_level_pivot")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Blankets lifespan 

# COMMAND ----------

import time
startTime = time.time()
sql = '''
  Select 
  left(BLANKETS_ID,7) AS Run_Number,
  left(BLANKETS_ID,10) AS Blanket_Serial_Number,
  Fact_PIP_IMPACT_RowID AS Blanket_RowID,
  Press_Serial_Number, 
  Replacement_DateTime ,
  ROR ,
  Consumable_Type ,
  Optimized_Lifespan,
  Is_Last_Replacement ,
  Consumable_Maturity ,
  DOA_Count ,
  DOP_Count ,
  Changed_Date_Time ,
  Replacement_Monthly_Date_Id ,
  ETL_Date ,
  Press_Classification ,
  Lifespan_Guidelines ,
  Click_Charge ,
  Ownership ,
  Product_Number ,
  Description ,
  Product_Group ,
  Press_Group ,
  Family_type ,
  Series ,
  Press_Segment ,
  Current_SW_Version_ID ,  
  Site_Region 
from blankets_db.lz_blanket_lifespan_hist
where length(BLANKETS_ID) = 13
AND Is_Lifespan_Official="YES"
AND Product_Group is not null
'''
ld_blanket_lifespan_run_df = spark.sql(sql)
# write table to parquet


executionTime = (time.time() - startTime)
print('Execution time in seconds: ' + str(round(executionTime,2)))

# COMMAND ----------

ld_blanket_lifespan_run_df.createOrReplaceTempView("ld_blanket_lifespan_run")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create Blankets lifespan stat

# COMMAND ----------




import time
startTime = time.time()
sql = '''
Select 
Run_Number
,Sum(Optimized_Lifespan) AS Total_Imperssion_Per_Run
,Count(Blanket_Serial_Number) AS Count_Blankets_Per_Run
,CAST(Avg(Optimized_Lifespan) AS INT) AS Avg_Lifespan_Per_Run
,percentile_approx(Optimized_Lifespan, 0.5) AS Median_Lifespan_Per_Run
,CAST(stddev(Optimized_Lifespan) AS INT) AS Lifespan_Stdev
FROM ld_blanket_lifespan_run BL
Group by 
Run_Number
'''
ld_blanket_lifespan_run_stat_df = spark.sql(sql)
# write table to parquet
##ld_blanket_lifespan_run_stat_df.write.parquet(LoadingPath + "/Blanket_lifespan/ld_blanket_lifespan_run_stat/",mode="overwrite")
executionTime = (time.time() - startTime)
print('Execution time in seconds: ' + str(round(executionTime,2)))


# COMMAND ----------

# MAGIC %md
# MAGIC Drop Table blankets_db.ld_blanket_lifespan_run_extended

# COMMAND ----------

dbutils.fs.rm(LoadingPath + "/Blanket_lifespan/ld_blanket_lifespan_run",True)

# COMMAND ----------


ld_blanket_lifespan_run_extended = ld_blanket_lifespan_run_df.join(ld_blanket_lifespan_run_stat_df,ld_blanket_lifespan_run_df.Run_Number == ld_blanket_lifespan_run_stat_df.Run_Number,"left").drop(ld_blanket_lifespan_run_stat_df.Run_Number)
ld_blanket_lifespan_run_extended.write.format("delta").option("path",LoadingPath + "/Blanket_lifespan/ld_blanket_lifespan_run_extended").mode("overwrite").saveAsTable("ld_blanket_lifespan_run_extended")

# COMMAND ----------


spark.sql("Show Tables from blankets_db").toPandas().head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create New Era Tables

# COMMAND ----------

import time
startTime = time.time()
sql = '''
SELECT
  Run_Number,
  Blanket_Serial_Number,
  Blanket_Legacy_Part_Nr ,
  Product_Engineering_Name
  from blankets_db.lz_new_era_hist
  where BLK_Quality_Status_Flag="1"
  AND Run_Number is not null
'''
ld_New_Era_df = spark.sql(sql)
# write table to parquet
# ld_New_Era_df.write.parquet(LoadingPath + "/New_Era/ld_New_Era/",mode="overwrite")
executionTime = (time.time() - startTime)
print('Execution time in seconds: ' + str(round(executionTime,2)))

# COMMAND ----------

ld_new_era_Yield_df = spark.sql(
'''select Run_Number,CAST(Sum(BLK_Quality_Status_Flag)/Count(Blanket_SEQ_NR) AS DECIMAL(5, 2)) AS Yield  from blankets_db.lz_new_era_hist
Group by Run_Number''')


# COMMAND ----------

dbutils.fs.rm(LoadingPath + "/New_Era/ld_New_Era/",recurse=True)

# COMMAND ----------

dbutils.fs.rm(certified_pit + "/pandas",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Drop table ce_Blankets_Production_lifespan

# COMMAND ----------

ld_New_Era =ld_New_Era_df.join(ld_new_era_Yield_df,ld_New_Era_df.Run_Number ==  ld_new_era_Yield_df.Run_Number,"left").drop(ld_new_era_Yield_df.Run_Number)
ld_New_Era.write.format("delta").option("path",LoadingPath + "/New_Era/ld_New_Era/").mode("overwrite").saveAsTable("ld_New_Eras")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   NE.Run_Number,
# MAGIC   NE.Blanket_Serial_Number,
# MAGIC   Blanket_Legacy_Part_Nr ,
# MAGIC   Product_Engineering_Name,
# MAGIC   Yield,
# MAGIC   Press_Serial_Number, 
# MAGIC   Replacement_DateTime ,
# MAGIC   ROR ,
# MAGIC   Consumable_Type ,
# MAGIC   Optimized_Lifespan,
# MAGIC   Is_Last_Replacement ,
# MAGIC   Consumable_Maturity ,
# MAGIC   DOA_Count ,
# MAGIC   DOP_Count ,
# MAGIC   Changed_Date_Time ,
# MAGIC   Replacement_Monthly_Date_Id ,
# MAGIC   ETL_Date ,
# MAGIC   Press_Classification ,
# MAGIC   Lifespan_Guidelines ,
# MAGIC   Click_Charge ,
# MAGIC   Ownership ,
# MAGIC   Product_Number ,
# MAGIC   Description ,
# MAGIC   Product_Group ,
# MAGIC   Press_Group ,
# MAGIC   Family_type ,
# MAGIC   Series ,
# MAGIC   Press_Segment ,
# MAGIC   Current_SW_Version_ID ,  
# MAGIC   Site_Region,
# MAGIC   Total_Imperssion_Per_Run,
# MAGIC   Count_Blankets_Per_Run,
# MAGIC   Avg_Lifespan_Per_Run,
# MAGIC   Median_Lifespan_Per_Run,
# MAGIC   Lifespan_Stdev,
# MAGIC   PR.*
# MAGIC   FROM ld_blanket_lifespan_run_extended BL
# MAGIC   inner join  ld_New_Eras  as NE
# MAGIC   on NE.Blanket_Serial_Number = BL.Blanket_Serial_Number
# MAGIC   inner join blankets_db.ld_parameters_run_level_pivot PR
# MAGIC   on NE.Run_Number = PR.Batch
# MAGIC   

# COMMAND ----------

# sql statement to derive summary customer stats
import time
startTime = time.time()
sql = '''

SELECT
  NE.Run_Number,
  NE.Blanket_Serial_Number,
  Blanket_Legacy_Part_Nr ,
  Product_Engineering_Name,
  Yield,
  Press_Serial_Number, 
  Replacement_DateTime ,
  ROR ,
  Consumable_Type ,
  Optimized_Lifespan,
  Is_Last_Replacement ,
  Consumable_Maturity ,
  DOA_Count ,
  DOP_Count ,
  Changed_Date_Time ,
  Replacement_Monthly_Date_Id ,
  ETL_Date ,
  Press_Classification ,
  Lifespan_Guidelines ,
  Click_Charge ,
  Ownership ,
  Product_Number ,
  Description ,
  Product_Group ,
  Press_Group ,
  Family_type ,
  Series ,
  Press_Segment ,
  Current_SW_Version_ID ,  
  Site_Region,
  Total_Imperssion_Per_Run,
  Count_Blankets_Per_Run,
  Avg_Lifespan_Per_Run,
  Median_Lifespan_Per_Run,
  Lifespan_Stdev,
  PR.*
  FROM ld_blanket_lifespan_run_extended BL
  inner join  ld_New_Eras  as NE
  on NE.Blanket_Serial_Number = BL.Blanket_Serial_Number
  inner join blankets_db.ld_parameters_run_level_pivot PR
  on NE.Run_Number = PR.Batch;
'''
Blankets_Production_lifespan_df = spark.sql(sql)
Blankets_Production_lifespan_df.write.format("delta") \
.option("path",certified_pit + "/Blankets_Production_lifespan/ce_Blankets_Production_lifespan/") \
.mode("overwrite") \
.saveAsTable("ce_Blankets_Production_lifespan")
#Blankets_Production_lifespan_df.write.parquet(certified_pit + "/Blankets_Production_lifespan/ce_Blankets_Production_lifespan/",mode="overwrite")
executionTime = (time.time() - startTime)
print('Execution time in seconds: ' + str(round(executionTime,2)))
#Parameters_run_level_dfp = Parameters_run_level_df.toPandas()

# COMMAND ----------



# COMMAND ----------

Parameters_run_level_dfp = spark.read.format("delta").load(certified_pit + "/Blankets_Production_lifespan/ce_Blankets_Production_lifespan/").toPandas()
Parameters_run_level_dfp.to_parquet("/dbfs/mnt/Blankets_model/azureml/Blankets_model/Dev/certified-pit/pandas/ce_Blankets_Production_lifespan_pandas.parquet")
Parameters_run_level_dfp.info()  

# COMMAND ----------

Parameters_run_level_dfp.info()

# COMMAND ----------

import pandas as pd
#pd.set_option('display.height', 500)
pd.set_option('display.max_rows', 500)
x = pd.DataFrame(Parameters_run_level_dfp.columns)
x


# COMMAND ----------

print(LoadingPath)

# COMMAND ----------

# must create the folder before
Parameters_run_level_dfp.to_parquet("/dbfs/mnt/Blankets_model/azureml/Blankets_model/Dev/certified-pit/ML_DF.parquet")

# COMMAND ----------

dbutils.fs.mkdirs("/dbfs/mnt/Blankets_model/azureml/Blankets_model/Dev/certified-pit/ML_DF")

# COMMAND ----------

x = spark.read.format("parquet").load("dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/certified-pit/ML_DF.parquet").toPandas()
Parameters_run_level_dfp.to_parquet("/dbfs/mnt/Blankets_model/azureml/Blankets_model/Dev/certified-pit/ML_DF.parquet")
x.head()  

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Run_Number,Blanket_Serial_Number,Blanket_Legacy_Part_Nr,Product_Engineering_Name from blankets_db.lz_new_era_hist
# MAGIC where Run_Number is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from (
# MAGIC SELECT CONCAT(BLANKETS_ID, "|", Fact_PIP_IMPACT_RowID,"|",Press_Serial_Number)  as LifespanKey,left(BLANKETS_ID,10) AS blanket_ID_short, * FROM blankets_db.lz_blanket_lifespan_hist BL
# MAGIC --  select NE.Run_Number, NE.Blanket_Legacy_Part_Nr,NE.Body_ID,NE.CSL_ID,NE.Product_Engineering_Name,BL.*,PR.*  from Blankets_lifespan BL
# MAGIC inner join  (
# MAGIC select distinct Run_Number,Blanket_Serial_Number,Blanket_Legacy_Part_Nr,Product_Engineering_Name from blankets_db.lz_new_era_hist
# MAGIC where Run_Number is not null
# MAGIC )  as NE
# MAGIC on NE.Blanket_Serial_Number = left(BL.BLANKETS_ID,10)
# MAGIC inner join blankets_db.parameters_run_level_pivot PR
# MAGIC on NE.Run_Number = PR.Batch
# MAGIC WHERE length(BLANKETS_ID) = 13
# MAGIC AND Run_Number is not null) limit 10 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from blankets_db.lz_blanket_lifespan_hist BL

# COMMAND ----------

# sql statement to derive summary customer stats
sql = '''

select * from (
SELECT CONCAT(BLANKETS_ID, "|", Fact_PIP_IMPACT_RowID,"|",Press_Serial_Number)  as LifespanKey,left(BLANKETS_ID,10) AS blanket_ID_short, * FROM blankets_db.lz_blanket_lifespan_hist BL
--  select NE.Run_Number, NE.Blanket_Legacy_Part_Nr,NE.Body_ID,NE.CSL_ID,NE.Product_Engineering_Name,BL.*,PR.*  from Blankets_lifespan BL
inner join  (
select distinct Run_Number,Blanket_Serial_Number,Blanket_Legacy_Part_Nr,Product_Engineering_Name from blankets_db.lz_new_era_hist
where Run_Number is not null
)  as NE
on NE.Blanket_Serial_Number = left(BL.BLANKETS_ID,10)
inner join blankets_db.parameters_run_level_pivot PR
on NE.Run_Number = PR.Batch
WHERE length(BLANKETS_ID) = 13
AND Run_Number is not null) limit 10;
'''

# capture stats in dataframe 
Parameters_run_level_df = spark.sql(sql)
Parameters_run_level_dfp2 = Parameters_run_level_df.toPandas()

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE blankets_db.lz_blanket_lifespan_hist

# COMMAND ----------

SELECT source, percentile_approx(value, 0.5) FROM df GROUP BY source

# COMMAND ----------

# MAGIC 
# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC Select 
# MAGIC left(BLANKETS_ID,7) AS Run_Number
# MAGIC ,Product_Group
# MAGIC ,Press_Group
# MAGIC ,Family_type
# MAGIC ,Series
# MAGIC ,Press_Segment
# MAGIC ,Product_Number
# MAGIC ,Click_Charge
# MAGIC ,Lifespan_Guidelines
# MAGIC ,Sum(Optimized_Lifespan) AS Total_Imperssion_Per_Run
# MAGIC ,Count(BLANKETS_ID) AS Count_Blankets_Per_Run
# MAGIC ,CAST(Avg(Optimized_Lifespan) AS INT) AS Avg_Lifespan_Per_Run
# MAGIC ,percentile_approx(Optimized_Lifespan, 0.5) AS Median_Lifespan_Per_Run
# MAGIC ,CAST(stddev(Optimized_Lifespan) AS INT) AS Stdev --OVER (PARTITION BY left (BLANKETS_ID,7) ORDER BY length(BLANKETS_ID)) AS stdev
# MAGIC FROM blankets_db.lz_blanket_lifespan_hist BL
# MAGIC WHERE length(BLANKETS_ID) = 13
# MAGIC AND Product_Group is not null
# MAGIC Group by 
# MAGIC left(BLANKETS_ID,7)
# MAGIC ,Product_Group
# MAGIC ,Press_Group
# MAGIC ,Family_type
# MAGIC ,Series
# MAGIC ,Press_Segment
# MAGIC ,Product_Number
# MAGIC ,Click_Charge
# MAGIC ,Lifespan_Guidelines
# MAGIC 
# MAGIC --AND Run_Number is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM blankets_db.lz_blanket_lifespan_hist BL

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from blankets_db.lz_new_era_hist
# MAGIC where Run_Number="NDU4651"

# COMMAND ----------

# MAGIC %sql
# MAGIC select Run_Number,CAST(Sum(BLK_Quality_Status_Flag)/Count(Blanket_SEQ_NR) AS DECIMAL(5, 2)) AS Yield  from blankets_db.lz_new_era_hist
# MAGIC Group by Run_Number

# COMMAND ----------

