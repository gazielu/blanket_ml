# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Phase 
# MAGIC 1. Transpose space production run from  long table to wide table
# MAGIC 2. Set new era + create new yield measure
# MAGIC 3. create blankets 

# COMMAND ----------

# MAGIC %md
# MAGIC # Tasks
# MAGIC 1. need to filter DOA & DOP

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial Notebook Set up

# COMMAND ----------

# MAGIC %run ../01-General/0-Mount_Indigo_Blob

# COMMAND ----------

MOUNTPOINT = "/mnt/Blankets_model"
SourcePath = MOUNTPOINT + "/azureml/Blankets_model"
RawPath = SourcePath + "/Dev/Raw"
ResearchPath = SourcePath + "/Dev/Research"
LandingPath = SourcePath + "/Dev/Landing"
LoadingPath = SourcePath + "/Dev/Loading"
certified_pit = SourcePath + "/Dev/certified-pit"
#display(dbutils.fs.ls(LoadingPath))

# COMMAND ----------

spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
from pyspark.sql.functions import *
from pyspark.sql.functions import col
import pandas as pd
import pyspark.pandas as ps


# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE blankets_db 

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transpose parameter table + Create Paramter Pivot
# MAGIC <br> create generic table by reduce spabe bi (parameters data) columns (only batch,paramter name, value) for pivoting table to wide table and then attached to the customer impression data 

# COMMAND ----------

sql = '''
  (SELECT   *,
   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Parameter_Name, '(name_edited)', ''),' ','_'),'_Median',''),'Median',''),'\n','') AS Parameter_Name_fix
   from blankets_db.lz_production_run_hist)
'''

df  = spark.sql(sql).drop(col("Parameter_Name")) \
.withColumnRenamed("Parameter_Name_fix","Parameter_Name")
     
#df.write.mode("overwrite").csv(ResearchPath+"/production_run/lz_parameters_names/")

# COMMAND ----------

sql = '''
  (SELECT   *,
   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Parameter_Name, '(name_edited)', ''),' ','_'),'_Median',''),'Median',''),'\n','') AS Parameter_Name_fix
   from blankets_db.lz_production_run_hist)
'''

df  = spark.sql(sql)
# df.write.format("csv").option("path",ResearchPath + # "/parameters_run/ld_fact_production_name_QA").mode("overwrite")#.saveAsTable("blankets_db.ld_fact_production_run_hist")#.sa
# .drop(col("Parameter_Name")) \
# .withColumnRenamed("Parameter_Name_fix","Parameter_Name")
     
df.write.mode("overwrite").csv(ResearchPath+"/production_run/lz_parameters_names/")

# COMMAND ----------


df.createOrReplaceTempView("TAB")
spark.sql("SELECT DISTINCT Parameter_Name,Parameter_Name_fix FROM TAB") \
.write.mode("overwrite") \
.option("header",True) \
.csv(ResearchPath+"/production_run/lz_parameters_names/")
#spark.sql("SELECT DISTINCT DEPARTMENT, SALARY FROM TAB").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS blankets_db.ld_fact_production_run_hist

# COMMAND ----------

df.write.format("parquet").option("path",LoadingPath + "/parameters_run/ld_fact_production_run_hist").mode("overwrite").saveAsTable("blankets_db.ld_fact_production_run_hist")#.sa

# COMMAND ----------


sql = '''
  (SELECT   Product,Batch,Parameter_Name || '_Mean' AS Parameter,SAMPLE_Mean AS Value FROM blankets_db.ld_production_run_hist)
  UNION
  (SELECT   Product,Batch,Parameter_Name || '_stdev' AS Parameter,SAMPLE_stdev AS Value FROM blankets_db.ld_production_run_hist)
  UNION
  (SELECT   Product,Batch,Parameter_Name || '_Median' AS Parameter,SAMPLE_Median AS Value FROM blankets_db.ld_production_run_hist)  
'''
# capture stats in dataframe 
parameters_run_level_Long_df = spark.sql(sql)


# COMMAND ----------

# MAGIC %python
# MAGIC # use pandas for 
# MAGIC parameters_run_level_pivot_df = parameters_run_level_Long_df.groupBy('Product','Batch').pivot('Parameter').avg('Value')

# COMMAND ----------

######## ONE TIME RUNNING #########
##################################
#### dealing with Nulls Parameters
#### copy the result and set to a list paramters
#### this is a one time running becouse we do not want to change the data  structre of the target dataset
parameters_run_level_pivot_dfp = parameters_run_level_pivot_df.toPandas()
df_columns_series = parameters_run_level_pivot_dfp.columns
Columns_less_the_two_unique_values = list(df_columns_series[parameters_run_level_pivot_dfp.nunique()<=1])
# parameters_run_level_pivot = parameters_run_level_pivot_dfp.drop(Columns_less_the_two_unique_values,axis=1)
# print(f'total columns droped:{len(Columns_less_the_two_unique_values)}\nlist of columns Droped by unique values: {Columns_less_the_two_unique_values}\ndata frame structure after droping {parameters_run_level_pivot.shape}')
print(Columns_less_the_two_unique_values)


# COMMAND ----------

Columns_less_the_two_unique_values = ['AIR_DR_IN_TEMP_Mean', 'AIR_DR_IN_TEMP_Median', 'AIR_DR_IN_TEMP_stdev', 'AIR_DR_IN_VELOCITY_Mean', 'AIR_DR_IN_VELOCITY_Median', 'AIR_DR_IN_VELOCITY_stdev', 'AIR_DR_OUT_TEMP_Mean', 'AIR_DR_OUT_TEMP_Median', 'AIR_DR_OUT_TEMP_stdev', 'AIR_DR_OUT_VELOCITY_Mean', 'AIR_DR_OUT_VELOCITY_Median', 'AIR_DR_OUT_VELOCITY_stdev', 'AIR_UV_LMP_COOLING_IN_TEMP_Mean', 'AIR_UV_LMP_COOLING_IN_TEMP_Median', 'AIR_UV_LMP_COOLING_IN_TEMP_stdev', 'AIR_UV_LMP_COOLING_IN_VELOCITY_Mean', 'AIR_UV_LMP_COOLING_IN_VELOCITY_Median', 'AIR_UV_LMP_COOLING_IN_VELOCITY_stdev', 'AIR_UV_LMP_COOLING_OUT_TEMP_Mean', 'AIR_UV_LMP_COOLING_OUT_TEMP_Median', 'AIR_UV_LMP_COOLING_OUT_TEMP_stdev', 'AIR_UV_LMP_COOLING_OUT_VELOCITY_Mean', 'AIR_UV_LMP_COOLING_OUT_VELOCITY_Median', 'AIR_UV_LMP_COOLING_OUT_VELOCITY_stdev', 'AIR_UV_QRZ_COOLING_TEMP_Mean', 'AIR_UV_QRZ_COOLING_TEMP_Median', 'AIR_UV_QRZ_COOLING_TEMP_stdev', 'AIR_UV_QRZ_COOLING_VELOCITY_Mean', 'AIR_UV_QRZ_COOLING_VELOCITY_Median', 'AIR_UV_QRZ_COOLING_VELOCITY_stdev', 'AIR_UV_SOLV_OUT_TEMP_Mean', 'AIR_UV_SOLV_OUT_TEMP_Median', 'AIR_UV_SOLV_OUT_TEMP_stdev', 'AIR_UV_SOLV_OUT_VELOCITY_Mean', 'AIR_UV_SOLV_OUT_VELOCITY_Median', 'AIR_UV_SOLV_OUT_VELOCITY_stdev', 'CTR_2_GRVR_ROLL_TORQUE_SP_Mean', 'CTR_2_GRVR_ROLL_TORQUE_SP_Median', 'CTR_2_GRVR_ROLL_TORQUE_SP_stdev', 'CTR_4_GRVR_SPEED_FDBK_Mean', 'CTR_4_GRVR_SPEED_FDBK_Median', 'CTR_4_GRVR_SPEED_FDBK_stdev', 'CTR_4_PUMP_SPEED_FDBK_Median', 'DR_EXHAUST_SPEED_FDBK_Mean', 'DR_EXHAUST_SPEED_FDBK_Median', 'DR_EXHAUST_SPEED_FDBK_stdev', 'DR_SUPPLY_SPEED_FB_Mean', 'DR_SUPPLY_SPEED_FB_Median', 'DR_SUPPLY_SPEED_FB_stdev', 'DR_VELOCITY_FB_Mean', 'DR_VELOCITY_FB_Median', 'DR_VELOCITY_FB_stdev', 'ENC_EXHAUST_SPEED_FDBK_Median', 'ITM-R_EXHAUST_SPEED_FDBK_Median', 'Laminator_IMP_Rel_Speed_%_Mean', 'Laminator_IMP_Rel_Speed_%_Median', 'Laminator_IMP_Rel_Speed_%_stdev', 'Laminator_IMP_Torque_%_Mean', 'Laminator_IMP_Torque_%_Median', 'Laminator_IMP_Torque_%_stdev', 'UV_LAMP_POWER_Mean', 'UV_LAMP_POWER_Median', 'UV_LAMP_POWER_stdev', 'UV_LAMP_TEMP_Mean', 'UV_LAMP_TEMP_Median', 'UV_LAMP_TEMP_stdev', 'UV_LAMP_VOLTS_Mean', 'UV_LAMP_VOLTS_Median', 'UV_LAMP_VOLTS_stdev']

# COMMAND ----------


# df_columns_series = parameters_run_level_pivot_df.columns
# Columns_less_the_two_unique_values = list(df_columns_series[parameters_run_level_pivot_dfp.nunique()<=1])
parameters_run_level_pivot = spark.createDataFrame(parameters_run_level_pivot_dfp.drop(Columns_less_the_two_unique_values,axis=1))
print(f'total columns droped:{len(Columns_less_the_two_unique_values)}\nlist of columns Droped by unique values: {Columns_less_the_two_unique_values}\ndata frame structure after droping  {parameters_run_level_pivot_temp.count(),len(parameters_run_level_pivot_temp.columns)}')

# COMMAND ----------


parameters_run_level_pivot.write.format("parquet").option("path",LoadingPath + "/parameters_run/ld_fact_parameters_run_level_pivot").mode("overwrite").saveAsTable("blankets_db.ld_fact_parameters_run_level_pivot")#.sa

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

ld_blanket_lifespan_run_df.createOrReplaceTempView("blankets_db.ld_blanket_lifespan_run")

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


# COMMAND ----------

# MAGIC %md
# MAGIC Drop Table blankets_db.ld_blanket_lifespan_run_extended

# COMMAND ----------


ld_blanket_lifespan_run_extended = ld_blanket_lifespan_run_df.join(ld_blanket_lifespan_run_stat_df,ld_blanket_lifespan_run_df.Run_Number == ld_blanket_lifespan_run_stat_df.Run_Number,"left").drop(ld_blanket_lifespan_run_stat_df.Run_Number)
ld_blanket_lifespan_run_extended.write.format("parquet").option("path",LoadingPath + "/Blanket_lifespan/ld_fact_blanket_lifespan_run_extended").mode("overwrite").saveAsTable("blankets_db.ld_fact_blanket_lifespan_run_extended")

# COMMAND ----------

ld_blanket_lifespan_run_extended.write.format("delta").option("path",LoadingPath + "/Blanket_lifespan/ld_fact_blanket_lifespan_run_extended_delta").mode("overwrite").saveAsTable("blankets_db.ld_fact_blanket_lifespan_run_extended_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create New Era Tables

# COMMAND ----------

ld_New_Era_df_temp = spark.read.format("parquet").load(LandingPath+"/New_Era/lz_fact_new_era_details_history/")

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


# COMMAND ----------

ld_new_era_Yield_df = spark.sql(
'''select Run_Number,cast(Count(Blanket_SEQ_NR)/Sum(BLK_Quality_Status_Flag) AS FLOAT) AS Yield from blankets_db.lz_new_era_hist --ld_New_Era_df_view
Group by Run_Number''')
#Sum(BLK_Quality_Status_Flag)*100/

# COMMAND ----------

ld_new_era_Yield_df.createOrReplaceTempView("ld_new_era_Yield_view")

# COMMAND ----------

ld_New_Era = ld_new_era_Yield_df.join(ld_New_Era_df,ld_New_Era_df.Run_Number == ld_new_era_Yield_df.Run_Number,"left").drop(ld_new_era_Yield_df.Run_Number)#.show(False)




# COMMAND ----------

ld_New_Era.write.format("parquet").option("path",LoadingPath + "/New_Era/ld_new_era/").mode("overwrite").saveAsTable("blankets_db.ld_fact_new_era")



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
  FROM blankets_db.ld_blanket_lifespan_run_extended BL
  inner join  blankets_db.ld_fact_new_era  as NE
  on NE.Blanket_Serial_Number = BL.Blanket_Serial_Number
  inner join blankets_db.ld_fact_parameters_run_level_pivot PR
  on NE.Run_Number = PR.Batch;
'''
Blankets_Production_lifespan_df = spark.sql(sql)

executionTime = (time.time() - startTime)


# COMMAND ----------

#Parameters_run_level_dfp = spark.read.format("parquet").load(certified_pit + "/Blankets_Production_lifespan/ce_Blankets_Production_lifespan_pandas_N.parquet/")


# COMMAND ----------

#Blankets_Production_lifespan_df.write.mode("overwrite").parquet(certified_pit + "/Blankets_Production_lifespan/ce_fact_Blankets_Production_lifespan_pandas.parquet")
Blankets_Production_lifespan_df.write.format("parquet") \
.option("path",certified_pit + "/Blankets_Production_lifespan/ce_fact_Blankets_Production_lifespan_pandas/") \
.mode("overwrite") \
.saveAsTable("blankets_db.ce_fact_Blankets_Production_lifespan_pandas")