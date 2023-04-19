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
# MAGIC SELECT   * FROM blankets_db.lz_production_run_hist limit 10

# COMMAND ----------


sql = '''
  (SELECT   Product,Batch,Parameter_Name || '_Mean' AS Parameter,SAMPLE_Mean AS Value FROM blankets_db.lz_production_run_hist)
  UNION
  (SELECT   Product,Batch,Parameter_Name || '_stdev' AS Parameter,SAMPLE_stdev AS Value FROM blankets_db.lz_production_run_hist)'''
# capture stats in dataframe 
parameters_run_level_Long_df = spark.sql(sql)
# write table to parquet
#parameters_run_level_Long_df.write.parquet(LoadingPath + "/parameters_run/ld_parameters_run_level_Long/",mode="overwrite")


# COMMAND ----------

# MAGIC %python
# MAGIC # use pandas for 
# MAGIC parameters_run_level_pivot_df = parameters_run_level_Long_df.groupBy('Product','Batch').pivot('Parameter').avg('Value')

# COMMAND ----------

parameters_run_level_pivot_df.toPandas()

# COMMAND ----------

# dealing with Nulls Parameters


parameters_run_level_pivot_dfp = parameters_run_level_pivot_df.toPandas()
df_columns_series = parameters_run_level_pivot_dfp.columns
#Columns_less_the_two_unique_values = list(df_columns_series[parameters_run_level_pivot_dfp.nunique()<=1])


# COMMAND ----------

Columns_less_the_two_unique_values = ['AIR_DR IN_TEMP_Mean', 'AIR_DR IN_TEMP_stdev', 'AIR_DR IN_VELOCITY_Mean', 'AIR_DR IN_VELOCITY_stdev', 'AIR_DR OUT_TEMP_Mean', 'AIR_DR OUT_TEMP_stdev', 'AIR_DR OUT_VELOCITY_Mean', 'AIR_DR OUT_VELOCITY_stdev', 'AIR_UV_LMP COOLING IN_TEMP_Mean', 'AIR_UV_LMP COOLING IN_TEMP_stdev', 'AIR_UV_LMP COOLING IN_VELOCITY_Mean', 'AIR_UV_LMP COOLING IN_VELOCITY_stdev', 'AIR_UV_LMP COOLING OUT_TEMP_Mean', 'AIR_UV_LMP COOLING OUT_TEMP_stdev', 'AIR_UV_LMP COOLING OUT_VELOCITY_Mean', 'AIR_UV_LMP COOLING OUT_VELOCITY_stdev', 'AIR_UV_QRZ COOLING_TEMP_Mean', 'AIR_UV_QRZ COOLING_TEMP_stdev', 'AIR_UV_QRZ COOLING_VELOCITY_Mean', 'AIR_UV_QRZ COOLING_VELOCITY_stdev', 'AIR_UV_SOLV OUT_TEMP_Mean', 'AIR_UV_SOLV OUT_TEMP_stdev', 'AIR_UV_SOLV OUT_VELOCITY_Mean', 'AIR_UV_SOLV OUT_VELOCITY_stdev', 'CSL_HP_SEAM_COUNTS_Mean', 'CSL_HP_SEAM_COUNTS_stdev', 'CTR_1_PUMP_SPEED_FDBK_Mean', 'CTR_1_PUMP_SPEED_FDBK_stdev', 'CTR_2_GRVR_ROLL_TORQUE_SP_Mean', 'CTR_2_GRVR_ROLL_TORQUE_SP_stdev', 'CTR_3_GRVR_ROLL_TORQUE_SP_Mean', 'CTR_3_GRVR_ROLL_TORQUE_SP_stdev', 'CTR_4_GRVR_ROLL_TORQUE_SP_Mean', 'CTR_4_GRVR_ROLL_TORQUE_SP_stdev', 'CTR_4_GRVR_SPEED_FDBK_Mean', 'CTR_4_GRVR_SPEED_FDBK_stdev', 'CTR_4_PUMP_SPEED_FDBK_Mean', 'CTR_4_PUMP_SPEED_FDBK_stdev', 'CTR_4_XFR_ROLL_TORQUE_SP_stdev', 'DR_EXHAUST_SPEED_FDBK_Mean', 'DR_EXHAUST_SPEED_FDBK_stdev', 'DR_SUPPLY_SPEED_FB_Mean', 'DR_SUPPLY_SPEED_FB_stdev', 'DR_VELOCITY_FB_Mean', 'DR_VELOCITY_FB_stdev', 'ENC_EXHAUST_SPEED_FDBK_Mean', 'ENC_EXHAUST_SPEED_FDBK_stdev', 'ITM-R_EXHAUST_SPEED_FDBK_Mean', 'ITM-R_EXHAUST_SPEED_FDBK_stdev', 'ITM-R_HOT_OIL_TEMP_FB_Mean', 'ITM-R_HOT_OIL_TEMP_FB_stdev', 'UV_LAMP_POWER_Mean', 'UV_LAMP_POWER_stdev', 'UV_LAMP_TEMP_Mean', 'UV_LAMP_TEMP_stdev', 'UV_LAMP_VOLTS_Mean', 'UV_LAMP_VOLTS_stdev']

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

#dbutils.fs.rm(LoadingPath + "/parameters_run/ld_parameters_run_level_pivot_N",True)

# COMMAND ----------

from pyspark.sql.functions import *
parameters_run_level_pivot.coalesce(8).write.parquet(LoadingPath + "/parameters_run/ld_parameters_run_level_pivot_N",mode="overwrite")
#parameters_run_level_pivot.write.format("delta").option("path",LandingPath + "/parameters_run/ld_parameters_run_level_pivot_N").mode("overwrite").saveAsTable("ld_parameters_run_level_pivot_N")#.saveAsTable("ld_parameters_run_level_pivot")
#parameters_run_level_pivot.write.format("delta").option("path",LoadingPath + "/parameters_run/ld_parameters_run_level_pivot_NN").mode("overwrite").saveAsTable("ld_parameters_run_level_pivot")

# COMMAND ----------

parameters_run_level_pivot.createOrReplaceTempView("ld_parameters_run_level_pivot")

# COMMAND ----------

# MAGIC %sql 
# MAGIC REFRESH table ld_parameters_run_level_pivot

# COMMAND ----------

# MAGIC %sql
# MAGIC select product,count(Batch) from ld_parameters_run_level_pivot 
# MAGIC Group by Product

# COMMAND ----------

# MAGIC %md
# MAGIC ## Blankets lifespan 

# COMMAND ----------



# COMMAND ----------

import time
startTime = time.time()
sql = '''
  Select 
  left(BLANKETS_ID,7) AS Run_Number,
  left(BLANKETS_ID,10) AS Blanket_Serial_Number,
  Blanket_IMPACT_RowID AS Blanket_RowID,
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
  --Press_Classification ,
  --Lifespan_Guidelines ,
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
--where length(BLANKETS_ID) = 13
--AND Is_Lifespan_Official="YES"
--AND Product_Group is not null
'''
ld_blanket_lifespan_run_df = spark.sql(sql)
# write table to parquet


executionTime = (time.time() - startTime)
#print('Execution time in seconds: ' + str(round(executionTime,2)))

# COMMAND ----------

ld_blanket_lifespan_run_df.createOrReplaceTempView("ld_blanket_lifespan_run")

# COMMAND ----------

# MAGIC %sql select * from blankets_db.lz_blanket_lifespan_hist limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select ld_blanket_lifespan_run

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create Blankets lifespan stat

# COMMAND ----------



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
#print('Execution time in seconds: ' + str(round(executionTime,2)))


# COMMAND ----------

# MAGIC %md
# MAGIC Drop Table blankets_db.ld_blanket_lifespan_run_extended

# COMMAND ----------

#dbutils.fs.rm(LoadingPath + "/Blanket_lifespan/ld_blanket_lifespan_run_extended",True)
dbutils.fs.rm(certified_pit + "/Blankets_Production_lifespan/ce_Blankets_Production_lifespan_N",True)


# COMMAND ----------

ld_blanket_lifespan_run_extended.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table ld_blanket_lifespan_run_extended

# COMMAND ----------


ld_blanket_lifespan_run_extended = ld_blanket_lifespan_run_df.join(ld_blanket_lifespan_run_stat_df,ld_blanket_lifespan_run_df.Run_Number == ld_blanket_lifespan_run_stat_df.Run_Number,"left").drop(ld_blanket_lifespan_run_stat_df.Run_Number)
ld_blanket_lifespan_run_extended.write.format("parquet").option("path",LoadingPath + "/Blanket_lifespan/ld_blanket_lifespan_run_extended").mode("overwrite").saveAsTable("ld_blanket_lifespan_run_extended")

# COMMAND ----------


spark.sql("Show Tables from blankets_db").toPandas().head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create New Era Tables

# COMMAND ----------

ld_New_Era_df_temp = spark.read.format("parquet").load(LandingPath + "/New_Era/New_era_details_Full.parquet/")

#ld_New_Era_df_temp =  spark.read.format("parquet").load("dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Landing/New_Era/New_era_details_Full.parquet/")
ld_New_Era_df_temp.createOrReplaceTempView("ld_New_Era_df_view")

# COMMAND ----------

import time
startTime = time.time()
sql = '''
SELECT
  Run_Number,
  Blanket_Serial_Number,
  Blanket_Legacy_Part_Nr ,
  Product_Engineering_Name
  from ld_New_Era_df_view
  where BLK_Quality_Status_Flag="1"
  AND Run_Number is not null
'''
ld_New_Era_df = spark.sql(sql)
#ld_New_Era_df.createOrReplaceTempView("ld_New_Era_df")
# write table to parquet
# ld_New_Era_df.write.parquet(LoadingPath + "/New_Era/ld_New_Era/",mode="overwrite")
executionTime = (time.time() - startTime)
#print('Execution time in seconds: ' + str(round(executionTime,2)))

# COMMAND ----------

ld_new_era_Yield_df = spark.sql(
'''select Run_Number,CAST(Sum(BLK_Quality_Status_Flag)/Count(Blanket_SEQ_NR) AS FLOAT) AS Yield  from ld_New_Era_df_view
Group by Run_Number''')


# COMMAND ----------

ld_new_era_Yield_df.createOrReplaceTempView("ld_new_era_Yield_view")

# COMMAND ----------

ld_new_era_Yield_df = spark.sql(
'''select Run_Number,(Sum(BLK_Quality_Status_Flag)/Count(Blanket_SEQ_NR)) AS Yield  from blankets_db.lz_new_era_hist
Group by Run_Number''')

# COMMAND ----------

#dbutils.fs.rm(LoadingPath + "/New_Era/ld_New_Era/",recurse=True)

# COMMAND ----------

#dbutils.fs.rm(certified_pit + "/pandas",True)

# COMMAND ----------

#%sql

#Drop table ce_Blankets_Production_lifespan

# COMMAND ----------

#%sql
#drop  VIEW  ld_New_Eras

# COMMAND ----------

ld_New_Era =ld_New_Era_df.join(ld_new_era_Yield_df,ld_New_Era_df.Run_Number ==  ld_new_era_Yield_df.Run_Number,"left").drop(ld_new_era_Yield_df.Run_Number)
#ld_New_Era.write.format("delta").option("path",LoadingPath + "/New_Era/ld_New_Era/").mode("overwrite").saveAsTable("ld_New_Eras")
ld_New_Era.createOrReplaceTempView("ld_New_Eras")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE ld_blanket_lifespan_run_extended

# COMMAND ----------

ld_New_Era_df.createOrReplaceTempView("ld_New_Eras_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from blankets_db.ld_parameters_run_level_pivot  limit 10 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) FROM blankets_db.ld_parameters_run_level_pivot

# COMMAND ----------

Blanketslifespan.coalesce(1).write.options(header='True', delimiter='\t') \
 .csv(certified_pit  +"\Blanketslifespan")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   --Product,
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
# MAGIC  
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
# MAGIC   inner join ld_parameters_run_level_pivot PR
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
  inner join ld_parameters_run_level_pivot PR
  on NE.Run_Number = PR.Batch;
'''
Blankets_Production_lifespan_df = spark.sql(sql)

Blankets_Production_lifespan_df.coalesce(8).write.parquet(certified_pit + "/Blankets_Production_lifespan/ce_Blankets_Production_lifespan_N",mode="overwrite")
#Blankets_Production_lifespan_df.write.format("parquet") \
#.option("path",certified_pit + "/Blankets_Production_lifespan/ce_Blankets_Production_lifespan_N/") \
#.mode("overwrite") 
#.saveAsTable("ce_Blankets_Production_lifespan_N")
#Blankets_Production_lifespan_df.write.parquet(certified_pit + "/Blankets_Production_lifespan/ce_Blankets_Production_lifespan/",mode="overwrite")
executionTime = (time.time() - startTime)
#print('Execution time in seconds: ' + str(round(executionTime,2)))
#Parameters_run_level_dfp = Parameters_run_level_df.toPandas()

# COMMAND ----------

azureml/Blankets_model/Dev/certified-pit/Blankets_Production_lifespan/ce_Blankets_Production_lifespan_pandas_N.parquet/

# COMMAND ----------

Parameters_run_level_dfp = spark.read.format("parquet").load(certified_pit + "/Blankets_Production_lifespan/ce_Blankets_Production_lifespan_pandas_N.parquet/")


# COMMAND ----------

Parameters_run_level_dfp.count()

# COMMAND ----------

Parameters_run_level_dfp.coalesce(1).write.options(header='True', delimiter='\t') \
 .csv(certified_pit  +"/Blanketslifespan_dataset")

# COMMAND ----------

Parameters_run_level_dfp.coalesce(1).write.parquet(certified_pit + "/Blankets_Production_lifespan/ce_Blankets_Production_lifespan_pandas_N.parquet",mode="overwrite")

# COMMAND ----------

Parameters_run_level_dfp.to_parquet("/dbfs/mnt/Blankets_model/azureml/Blankets_model/Dev/certified-pit/pandas/ce_Blankets_Production_lifespan_pandas_N.parquet")
Parameters_run_level_dfp.info()  