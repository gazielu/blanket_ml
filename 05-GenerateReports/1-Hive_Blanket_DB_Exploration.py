# Databricks notebook source
# MAGIC %md
# MAGIC ### Describe Database 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED BLANKETS_DB

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC DROP SCHEMA IF EXISTS ch1 CASCADE;
# MAGIC 
# MAGIC SHOW databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN blankets_db

# COMMAND ----------

# MAGIC  %sql
# MAGIC --  list table or view  
# MAGIC USE BLANKETS_DB;
# MAGIC SHOW TABLE EXTENDED LIKE '*'
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE blankets_lifespan IS 'This Table contain all the impression / lifesan details'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES blankets_lifespan

# COMMAND ----------

# display(dbutils.fs.ls(SourcePath))
# display(dbutils.fs.ls(activityRawPath))
#display(dbutils.fs.ls(activityStagingPath))


# display(dbutils.fs.ls(activityStagingPath + "/Production_run_gemini"))

display(dbutils.fs.ls("dbfs:/mnt/Blankets_model/azureml/Blankets_model/Dev/Loading/New_Era_Run_Summary/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --New_Era_Run_Summary.printSchema() if it was a dataframe
# MAGIC -- new_era_run_summary
# MAGIC 
# MAGIC DESCRIBE FORMATTED BLANKETS_DB.new_era_run_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED BLANKETS_DB.blankets_lifespan

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE BLANKETS_DB.blankets_lifespan change COLUMN Consumable_SN COMMENT "this is a comment"

# COMMAND ----------

# MAGIC %md
# MAGIC Explore dataframe with simply create tempview

# COMMAND ----------

# MAGIC %md
# MAGIC convert pandas DF to Spark DF  >  like if we read excel
# MAGIC New_Era_Run_Details_view = spark.createDataFrame(New_Era_Run_Details)
# MAGIC 
# MAGIC present Spark DF as queriable view
# MAGIC New_Era_Run_Details.createOrReplaceTempView('New_Era') 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generals Data validation Queries

# COMMAND ----------

# Count Validation

New_Era_Counter = spark.sql("SELECT count(*) From BLANKETS_DB.new_era_run_summary").first()[0]
blankets_lifespan = spark.sql("SELECT count(*) From BLANKETS_DB.blankets_lifespan").first()[0]
production_run_gemini = spark.sql("SELECT count(*) From BLANKETS_DB.production_run_gemini").first()[0]
print(New_Era_Counter,blankets_lifespan,production_run_gemini)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tables Explorations

# COMMAND ----------

# MAGIC %md
# MAGIC ## show tables

# COMMAND ----------

# MAGIC  %sql
# MAGIC SHOW TABLES FROM BLANKETS_DB

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production_run_gemini

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM BLANKETS_DB.Production_run_gemini LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as counter from (
# MAGIC select distinct Batch from BLANKETS_DB.Production_run_gemini) as a

# COMMAND ----------




# Generate new loans with dollar amounts 
loans = sql("SELECT Batch,Parameter_Name || '_Mean' AS Parameter,SAMPLE_Mean AS Value FROM Production_run_gemini")
#UNION
# - SELE
#sql("SELECT Batch,Parameter_Name || 'stdev' AS Parameter,SAMPLE_Mean AS Value FROM Production_run_gemini")CT Batch,Parameter_Name || '_stdev' AS Parameter,SAMPLE_stdev AS Value FROM Production_run_gemini")
display(loans)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate new loans with dollar amounts
# MAGIC SELECT Batch,Parameter_Name || '_Mean' AS Parameter,SAMPLE_Mean AS Value FROM BLANKETS_DB.Production_run_gemini
# MAGIC UNION
# MAGIC SELECT Batch,Parameter_Name || 'stdev' AS Parameter,SAMPLE_stdev AS Value FROM BLANKETS_DB.Production_run_gemini

# COMMAND ----------

# sql statement to derive summary customer stats
sql = '''
SELECT   Batch,Parameter_Name || '_Mean' AS Parameter,SAMPLE_Mean AS Value FROM BLANKETS_DB.Production_run_gemini limit 10
UNION
SELECT   Batch,Parameter_Name || 'stdev' AS Parameter,SAMPLE_stdev AS Value FROM BLANKETS_DB.Production_run_gemini limit 10
  '''

# capture stats in dataframe 
Parameters_run_level = spark.sql(sql)

# display stats
display(Parameters_run_level)  

# COMMAND ----------

Parameters_run_level.coalesce(2).write.parquet(activityStagingPath + "/Parameters_run_level",mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC CREATE DATABASE IF NOT EXISTS BLANKETS_DB;
# MAGIC  
# MAGIC USE BLANKETS_DB;
# MAGIC  
# MAGIC DROP TABLE IF EXISTS Parameters_run_level;
# MAGIC CREATE TABLE IF NOT EXISTS Parameters_run_level
# MAGIC USING parquet
# MAGIC OPTIONS (path "/mnt/Blankets_model/azureml/Blankets_model/Dev/Staging/Parameters_run_level/");
# MAGIC ANALYZE TABLE Parameters_run_level COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from BLANKETS_DB.Parameters_run_level

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as counter from (
# MAGIC select distinct Batch from BLANKETS_DB.Parameters_run_level) as a
# MAGIC --- 1278 run

# COMMAND ----------

# MAGIC %md
# MAGIC ## parameters_run_level

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as counter from (
# MAGIC select distinct Batch from BLANKETS_DB.Parameters_run_level) as a

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * From BLANKETS_DB.parameters_run_level --where Batch ="NDU3952" 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW parameters_run_level_test AS SELECT * From BLANKETS_DB.parameters_run_level where Batch ="KMV3809" 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parameters_run_level_test

# COMMAND ----------

# MAGIC %md
# MAGIC ## parameters_run_level_pivot

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from BLANKETS_DB.parameters_run_level_pivot

# COMMAND ----------

# MAGIC %md
# MAGIC ## new_era_run_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Count distinct run" as case, count(*) as counter from (
# MAGIC select distinct RUN from BLANKETS_DB.new_era_run_summary) as a

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE BLANKETS_DB.new_era_run_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- case 1 > there is run with 1340 rows  we need to investage duplication or number of serial number  > found 547 as serial number and 
# MAGIC -- need to test   
# MAGIC --RUN	Blanket Serial Number	Blanket_SEQ_NR	Plant Id	Quality Status Id	Pareto First Batch Entity Id	Pareto Parent Batch 
# MAGIC --NDU3952	                        547	          3248	22527003	null	null	133221-01-01T00:00:00.000+0000	133222	Series 3	
# MAGIC 
# MAGIC SELECT *, Year('Source System Modified DateTime') From BLANKETS_DB.new_era_run_summary -- where RUN="NDU3952"  

# COMMAND ----------

sql2 = '''
SELECT  RUN, count("Blanket Serial Number") AS blankets_per_run_Counter FROM BLANKETS_DB.new_era_run_summary
Group by RUN
Order By count("Blanket Serial Number") desc
  '''

# capture stats in dataframe 
blankets_per_serial = spark.sql(sql2)

# display stats
display(blankets_per_serial)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## new_era_run_summary_full

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct (Product_Engineering_Name)  From BLANKETS_DB.new_era_run_summary_full
# MAGIC where Product_Engineering_Name like "%Gem%"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Product_Engineering_Name,count( RUN)  From BLANKETS_DB.new_era_run_summary_full
# MAGIC where Product_Engineering_Name like "%Gem%"
# MAGIC group by Product_Engineering_Name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Product_Engineering_Name,count(distinct RUN)  From BLANKETS_DB.new_era_run_summary_full
# MAGIC where Product_Engineering_Name like "%Gem%"
# MAGIC group by Product_Engineering_Name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct Product_Engineering_Name, RUN  From BLANKETS_DB.new_era_run_summary_full
# MAGIC where Product_Engineering_Name like "%Gem%"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count (distinct RUN)  From BLANKETS_DB.new_era_run_summary_full
# MAGIC where Product_Engineering_Name like "%Gem%"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * From BLANKETS_DB.new_era_run_summary_full
# MAGIC where Product_Engineering_Name="GEMINI3"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Blankets lifespan

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * From BLANKETS_DB.new_era_run_summary where RUN="NDU3952"  

# COMMAND ----------

# MAGIC %md # Pivoting with Multiple Pivot Columns
# MAGIC Getting monthly average high and average low temperatures with (month, hi/lo) as columns and year as rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parameters_run_level_test

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC   SELECT Batch, month(date) month, temp, flag
# MAGIC   FROM (
# MAGIC     SELECT date, temp, 'H' as flag
# MAGIC     FROM high_temps
# MAGIC     UNION ALL
# MAGIC     SELECT date, temp, 'L' as flag
# MAGIC     FROM low_temps
# MAGIC   )
# MAGIC   WHERE date BETWEEN DATE '2015-01-01' AND DATE '2018-08-31'
# MAGIC )
# MAGIC PIVOT (
# MAGIC   CAST(avg(temp) AS DECIMAL(4, 1))
# MAGIC   FOR (month, flag) in (
# MAGIC     (6, 'H') JUN_hi, (6, 'L') JUN_lo,
# MAGIC     (7, 'H') JUL_hi, (7, 'L') JUL_lo,
# MAGIC     (8, 'H') AUG_hi, (8, 'L') AUG_lo,
# MAGIC     (9, 'H') SEP_hi, (9, 'L') SEP_lo
# MAGIC   )
# MAGIC )
# MAGIC ORDER BY year DESC

# COMMAND ----------

print('----- Count distinct row in paramters ------')
print("Distinct Count: " + str(Parameters_run_level.distinct().count()))


# COMMAND ----------

from pyspark.sql.functions import countDistinct
df2=Parameters_run_level.select(countDistinct("Parameter"))
df2.show()
print("Distinct Count of Department & Salary: "+ str(df2.collect()[0][0]))

# COMMAND ----------

New_Era_Run_Details.createOrReplaceTempView('New_Era') 

# COMMAND ----------

# MAGIC %sql
# MAGIC USE BLANKETS_DB;
# MAGIC SELECT Batch,count(*) FROM Production_run_gemini
# MAGIC group by Batch;