# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC We will run various reports and visualize                          

# COMMAND ----------

from pyspark.sql.functions import *

MOUNTPOINT = "/mnt/Blankets_model"
SourcePath = MOUNTPOINT + "/azureml/Blankets_model"
RawPath = SourcePath + "/Dev/Raw"
ResearchPath = SourcePath + "/Dev/Research"
LandingPath = SourcePath + "/Dev/Landing"
LoadingPath = SourcePath + "/Dev/Loading"
certified_pit = SourcePath + "/Dev/certified-pit"
#display(dbutils.fs.ls(LoadingPath))


# COMMAND ----------


parameters_run_level_pivot = spark.read.format("parquet").load(LoadingPath + "/parameters_run/ld_parameters_run_level_pivot").write.saveAsTable("test")


# COMMAND ----------

# MAGIC %sql
# MAGIC USE BLANKETS_DB

# COMMAND ----------



spark.read.format("parquet").load(LoadingPath + "/parameters_run/ld_parameters_run_level_pivot").write.saveAsTable("test")

# COMMAND ----------

blankets_lifespan = spark.read.format("parquet").load(LoadingPath + "/parameters_run/ld_parameters_run_level_pivot")

# COMMAND ----------

blankets_lifespan.createOrReplaceTempView("ld_blankets_lifespan_Temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from blankets_db
# MAGIC     --WHERE data_type = 'DOUBLE'
# MAGIC       --where table_schema = 'information_schema';

# COMMAND ----------

parameters_run_level_pivot.createOrReplaceTempView("parameters_run_level_pivot")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parameters_run_level_pivot limit 10 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.  Trip count by taxi type

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   taxi_type,
# MAGIC   count(*) as trip_count
# MAGIC from 
# MAGIC   taxi_db.taxi_trips_mat_view
# MAGIC group by taxi_type

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   taxi_type,
# MAGIC   count(*) as trip_count
# MAGIC from 
# MAGIC   taxi_db.taxi_trips_mat_view
# MAGIC group by taxi_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.  Revenue including tips by taxi type

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   taxi_type, sum(total_amount) revenue
# MAGIC from 
# MAGIC   taxi_db.taxi_trips_mat_view
# MAGIC group by taxi_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.  Revenue share by taxi type

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   taxi_type, sum(total_amount) revenue
# MAGIC from 
# MAGIC   taxi_db.taxi_trips_mat_view
# MAGIC group by taxi_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.  Trip count trend between 2013 and 2016

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   taxi_type, sum(total_amount) revenue
# MAGIC from 
# MAGIC   taxi_db.taxi_trips_mat_view
# MAGIC group by taxi_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.  Trip count trend by month, by taxi type, for 2016

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   taxi_type,
# MAGIC   trip_month as month,
# MAGIC   count(*) as trip_count
# MAGIC from 
# MAGIC   taxi_db.taxi_trips_mat_view
# MAGIC where 
# MAGIC   trip_year=2016
# MAGIC group by taxi_type,trip_month
# MAGIC order by trip_month 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.  Average trip distance by taxi type

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   taxi_type, round(avg(trip_distance),2) as trip_distance_miles
# MAGIC from 
# MAGIC   taxi_db.taxi_trips_mat_view
# MAGIC group by taxi_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.  Average trip amount by taxi type

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   taxi_type, round(avg(total_amount),2) as avg_total_amount
# MAGIC from 
# MAGIC   taxi_db.taxi_trips_mat_view
# MAGIC group by taxi_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.  Trips with no tip, by taxi type

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   taxi_type, count(*) tipless_count
# MAGIC from 
# MAGIC   taxi_db.taxi_trips_mat_view
# MAGIC where tip_amount=0
# MAGIC group by taxi_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.  Trips with no charge, by taxi type

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   taxi_type, count(*) as transactions
# MAGIC from 
# MAGIC   taxi_db.taxi_trips_mat_view
# MAGIC where
# MAGIC   payment_type_description='No charge'
# MAGIC   and total_amount=0.0
# MAGIC group by taxi_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.  Trips by payment type

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   payment_type_description as Payment_type, count(*) as transactions
# MAGIC from 
# MAGIC   taxi_db.taxi_trips_mat_view
# MAGIC group by payment_type_description

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.  Trip trend by pickup hour for yellow taxi in 2016

# COMMAND ----------

# MAGIC %sql
# MAGIC select pickup_hour,count(*) as trip_count
# MAGIC from taxi_db.yellow_taxi_trips_curated
# MAGIC where trip_year=2016
# MAGIC group by pickup_hour
# MAGIC order by pickup_hour

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.  Top 3 yellow taxi pickup-dropoff zones for 2016

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC   (
# MAGIC   select 
# MAGIC     pickup_zone,dropoff_zone,count(*) as trip_count
# MAGIC   from 
# MAGIC     taxi_db.yellow_taxi_trips_curated
# MAGIC   where 
# MAGIC     trip_year=2016
# MAGIC   and
# MAGIC     pickup_zone is not null and pickup_zone<>'NV'
# MAGIC   and 
# MAGIC     dropoff_zone is not null and dropoff_zone<>'NV'
# MAGIC   group by pickup_zone,dropoff_zone
# MAGIC   order by trip_count desc
# MAGIC   ) x
# MAGIC limit 3