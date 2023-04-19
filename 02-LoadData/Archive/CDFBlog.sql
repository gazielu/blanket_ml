-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Simplify Your Medallion Architecture with Delta Lake’s CDF Feature
-- MAGIC 
-- MAGIC ### Overview
-- MAGIC The medallion architecture takes raw data landed from source systems and refines the data through bronze, silver and gold tables. It is an architecture that the MERGE operation and log versioning in Delta Lake make possible. Change data capture (CDC) is a use case that we see many customers implement in Databricks. We are happy to announce an exciting new Change data feed (CDF) feature in Delta Lake that makes this architecture even simpler to implement!
-- MAGIC 
-- MAGIC The following example ingests financial data. Estimated Earnings Per Share (EPS) is financial data from analysts predicting what a company’s quarterly earnings per share will be. The raw data can come from many different sources and from multiple analysts for multiple stocks. The data is simply inserted into the bronze table, it will  change in the silver and then aggregate values need to be recomputed in the gold table based on the changed data in the silver. 
-- MAGIC 
-- MAGIC While these transformations can get complex, thankfully now the row based CDF feature can be simple and efficient but how do you use it? Let’s dig in!
-- MAGIC 
-- MAGIC <img src="https://databricks.com/wp-content/uploads/2021/05/cdf-blog-img-1-rev.png">

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create the tables with CDF enabled
-- MAGIC We must first create the bronze, silver and gold tables with CDF enabled by setting the table property using the following statement on each table
-- MAGIC 
-- MAGIC *TBLPROPERTIES (delta.enableChangeDataFeed = true)*

-- COMMAND ----------

CREATE TABLE bronze_eps 
  (date STRING, stock_symbol STRING, analyst INT, estimated_eps DOUBLE) 
  USING DELTA
  TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE silver_eps 
  (date STRING, stock_symbol STRING, analyst INT, estimated_eps DOUBLE) 
  USING DELTA
  TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE gold_consensus_eps 
  (date STRING, stock_symbol STRING, consensus_eps DOUBLE) 
  USING DELTA
  TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- COMMAND ----------

-- DBTITLE 1,Code used to build the 1st eps data set to import into the bronze_eps table
-- MAGIC %python
-- MAGIC df = spark.createDataFrame(
-- MAGIC     [('3/1/2021','a',1,2.2),\
-- MAGIC      ('3/1/2021','a',2,2.0),\
-- MAGIC      ('3/1/2021','b',1,1.3),\
-- MAGIC      ('3/1/2021','b',2,1.2),\
-- MAGIC      ('3/1/2021','c',1,3.5),\
-- MAGIC      ('3/1/2021','c',2,2.6)],
-- MAGIC     ('date','stock_symbol','analyst','estimated_eps'))
-- MAGIC 
-- MAGIC df.createOrReplaceTempView("bronze_eps_march_dataset")

-- COMMAND ----------

-- DBTITLE 1,Insert the dataset into the bronze table
INSERT INTO bronze_eps TABLE bronze_eps_march_dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Querying Row Changes By Table
-- MAGIC We query the changes to the table by using the *table_changes* operation

-- COMMAND ----------

SELECT * FROM table_changes('bronze_eps', 1)

-- COMMAND ----------

-- DBTITLE 1,Since the silver and gold tables have no rows yet, we will insert the values for now
INSERT INTO silver_eps 
SELECT date, stock_symbol, analyst, estimated_eps
FROM table_changes('bronze_eps', 1)

-- COMMAND ----------

-- DBTITLE 1,CDF results for the silver table
SELECT * FROM table_changes('silver_eps', 1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Updating The Gold Table Aggregate Values *ONLY* For The Rows Needed
-- MAGIC While we are still only inserting and not using the merge operation, the SQL statement below gives us an idea of how to use only the changed rows in the silver table to determine which rows in the gold table we need to aggregate.

-- COMMAND ----------

INSERT INTO gold_consensus_eps 
SELECT silver_eps.date, silver_eps.stock_symbol, AVG(estimated_eps) as consensus_eps
FROM silver_eps
INNER JOIN (SELECT DISTINCT date, stock_symbol FROM table_changes('silver_eps', 1)) AS silver_cdf
  ON silver_eps.date = silver_cdf.date
  AND silver_eps.stock_symbol = silver_cdf.stock_symbol
GROUP BY silver_eps.date, silver_eps.stock_symbol

-- COMMAND ----------

SELECT * FROM gold_consensus_eps

-- COMMAND ----------

-- DBTITLE 1,Code used to build the 2nd eps data set to import into the bronze_eps table
-- MAGIC %python
-- MAGIC df = spark.createDataFrame(
-- MAGIC     [('3/1/2021','a',2,2.4),\
-- MAGIC      ('4/1/2021','a',1,2.3),\
-- MAGIC      ('4/1/2021','a',2,2.1),\
-- MAGIC      ('4/1/2021','b',1,1.3),\
-- MAGIC      ('4/1/2021','b',2,1.2),\
-- MAGIC      ('4/1/2021','c',1,3.5),\
-- MAGIC      ('4/1/2021','c',2,2.6)],
-- MAGIC     ('date','stock_symbol','analyst','estimated_eps'))
-- MAGIC 
-- MAGIC df.createOrReplaceTempView("bronze_eps_april_dataset")

-- COMMAND ----------

-- DBTITLE 1,Insert the dataset into the bronze table
INSERT INTO bronze_eps TABLE bronze_eps_april_dataset

-- COMMAND ----------

-- DBTITLE 1,Most of the new data is for a new month but as you can see, there is an update too
SELECT * FROM table_changes('bronze_eps', 2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Our First Merge Operation
-- MAGIC Since we now may have updates and inserts we use a MERGE operation. As you can see though, we will *ONLY* upsert rows in the silver table that have changed in the bronze.

-- COMMAND ----------

MERGE INTO silver_eps
USING 
  (SELECT * FROM table_changes('bronze_eps', 2))
  AS bronze_cdf
ON bronze_cdf.date = silver_eps.date AND
  bronze_cdf.stock_symbol = silver_eps.stock_symbol AND
  bronze_cdf.analyst = silver_eps.analyst
WHEN MATCHED THEN
  UPDATE SET silver_eps.estimated_eps = bronze_cdf.estimated_eps
WHEN NOT MATCHED
  THEN INSERT (date, stock_symbol, analyst, estimated_eps) VALUES (date, stock_symbol, analyst, estimated_eps)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### New Change Types
-- MAGIC As you can see, we now have change types *update_preimage* and *update_postimage* so we can compare the updated values if needed. There is also a *delete* change type but we don't need to account for it in this case.

-- COMMAND ----------

SELECT * FROM table_changes('silver_eps', 2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A Complex Aggregation In a MERGE Operation
-- MAGIC Aggregate statements to update or insert gold tables can become complex but in this case, the *table_changes* operation keeps the aggregate statement simple and efficient.

-- COMMAND ----------

-- We only want to update for the dates and stocks that have changed
-- so in the SQL below we do a SELECT DISTINCT FROM table_changes...
SELECT DISTINCT date, stock_symbol FROM table_changes('silver_eps', 2)

-- COMMAND ----------

MERGE INTO gold_consensus_eps 
USING
  (SELECT silver_eps.date, silver_eps.stock_symbol, AVG(estimated_eps) as consensus_eps
  FROM silver_eps
  INNER JOIN (SELECT DISTINCT date, stock_symbol FROM table_changes('silver_eps', 2)) AS silver_cdf
    ON silver_eps.date = silver_cdf.date
    AND silver_eps.stock_symbol = silver_cdf.stock_symbol
  GROUP BY silver_eps.date, silver_eps.stock_symbol) as silver_cdf_agg
ON silver_cdf_agg.date = gold_consensus_eps.date AND
  silver_cdf_agg.stock_symbol = gold_consensus_eps.stock_symbol
WHEN MATCHED THEN
  UPDATE SET gold_consensus_eps.consensus_eps = silver_cdf_agg.consensus_eps
WHEN NOT MATCHED
  THEN INSERT (date, stock_symbol, consensus_eps) VALUES (date, stock_symbol, consensus_eps)

-- COMMAND ----------

-- DBTITLE 1,Only the row that needs to be updated is
SELECT * FROM table_changes('gold_consensus_eps', 2)

-- COMMAND ----------

-- DBTITLE 1,The full gold table efficiently and simply updated using the CDF feature!
SELECT * FROM gold_consensus_eps

-- COMMAND ----------

-- DBTITLE 1,Clean Up - Drop the created tables
DROP TABLE bronze_eps;
DROP TABLE silver_eps;
DROP TABLE gold_consensus_eps;