# Databricks notebook source
# MAGIC %md
# MAGIC ## Add variables

# COMMAND ----------

# MAGIC %run ../01-General/0-Mount_Indigo_Blob

# COMMAND ----------

MOUNTPOINT = "/mnt/Blankets_model"
SourcePath = MOUNTPOINT + "/azureml/Blankets_model"
RawPath = SourcePath + "/Dev/Raw"
ResearchPath = SourcePath + "/Dev/Research"
LandingPath = SourcePath + "/Dev/Landing"
LoadingPath = SourcePath + "/Dev/Loading"
display(dbutils.fs.ls(LandingPath))

# COMMAND ----------

# MAGIC %md
# MAGIC # Blankets model pre Processing
# MAGIC following the import data with remove duplication

# COMMAND ----------

# MAGIC %md
# MAGIC # mission
# MAGIC 1. add scaler 
# MAGIC 2. pickle df & scaler
# MAGIC 3. add status

# COMMAND ----------

import pickle
import pandas as pd
import numpy as np
import os
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import pickle
#from scipy.stats import zscore
%matplotlib inline
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# COMMAND ----------

import pyspark.pandas as ps
parameters_run_level_pivot_temp = pd.read_parquet("/dbfs/mnt/Blankets_model/azureml/Blankets_model/Dev/Loading/parameters_run/parameters_run_level_pivot")
type(parameters_run_level_pivot_temp)

# COMMAND ----------

# MAGIC %fs ls "/mnt/Blankets_model/azureml/Blankets_model/Dev/Loading/parameters_run/parameters_run_level_pivot"

# COMMAND ----------

parameters_run_level_pivot_temp.info()

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Deleting o and 1 unique value count</h4>

# COMMAND ----------

#check unique value filoceach column > if there is 0 or 1 values its mean that this features as no realtion to the output
uniqueValues_series = parameters_run_level_pivot_temp.nunique(dropna=True)
print('Count of unique value sin each column :')
print(uniqueValues_series.sort_values().head(100))

# COMMAND ----------

### Find columns with just 0 or 1 unique value ###
df_columns_series = parameters_run_level_pivot_temp.columns
Columns_less_the_two_unique_values = list(df_columns_series[parameters_run_level_pivot_temp.nunique()<=1])
parameters_run_level_pivot = parameters_run_level_pivot_temp.drop(Columns_less_the_two_unique_values,axis=1)
print(f'total columns droped:{len(Columns_less_the_two_unique_values)}\nlist of columns Droped by unique values: {Columns_less_the_two_unique_values}\ndata frame structure after droping {parameters_run_level_pivot.shape}')

# COMMAND ----------


df = df_after_drop_null
df.head()



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <h4>component of droping columns by threshold</h4>

# COMMAND ----------

##### component of droping columns  #########
threshold = 60
Columns_do_drop_by_empty_threshold = df.loc[:,list((100*(df.isnull().sum()/len(df.index))>=threshold))].columns
Columns_do_drop_by_empty_threshold.values.tolist()
Columns_do_drop_by_empty_threshold
df_after_drop_by_Column_threshold = df.drop(Columns_do_drop_by_empty_threshold,axis=1)
print(f'total columns droped:{len(Columns_do_drop_by_empty_threshold)}\nlist of columns Droped null thershold: {Columns_less_the_two_unique_values}\ndata frame structure after droping {df_after_drop_by_Column_threshold.shape}')


# COMMAND ----------

# MAGIC %md
# MAGIC <h4># Filter data Frame By % empty columns in a rows</h4>

# COMMAND ----------

# Filter data Frame By % empty columns in a rows
numberofcolumn = len(df_after_drop_by_Column_threshold.columns)
Thershold = 0.4
numberofnonnacolumns = round(numberofcolumn*Thershold)
#numberofnonnacolumns
df_after_droping_null_by_empty_rows = df_after_drop_by_Column_threshold.dropna(thresh=numberofnonnacolumns)  
df_after_droping_null_by_empty_rows.dropna(thresh=numberofnonnacolumns).shape
print(f'total rows droped by threshold:{df_after_drop_by_Column_threshold.shape[0]-len(df_after_droping_null_by_empty_rows)}\nlist of columns Droped null thershold: {Columns_less_the_two_unique_values}\ndata frame structure after droping {df_after_droping_null_by_empty_rows.shape}')

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>create new column for yearmonth & Duration</h4>
# MAGIC <a link>https://towardsdatascience.com/working-with-datetime-in-pandas-dataframe-663f7af6c587</link>

# COMMAND ----------

df_after_droping_null_by_empty_rows.loc[:,'Start_year_month'] = ((df_after_droping_null_by_empty_rows['Start'].dt.year)*100+df_after_droping_null_by_empty_rows['Start'].dt.month)
df_after_droping_null_by_empty_rows.loc[:,'Diffrence in Minutes']  = (df_after_droping_null_by_empty_rows['End  ']-df_after_droping_null_by_empty_rows['Start']).astype('timedelta64[h]')

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Featuress to Drop - no relevents to the Model</h5>

# COMMAND ----------

### Featuress to Drop - no relevents to the Model
##################
Non_relevant_features = ['Process P/N','Process Stage Name','Start','End  ']
#Non_relevant_features = ['Process P/N','Process Stage Name','Process S/N']
df_after_droping_Non_relevant_features = df_after_droping_null_by_empty_rows.drop(Non_relevant_features,axis=1)  
df_after_droping_Non_relevant_features.shape


# COMMAND ----------

pd.get_option("display.max_rows")

# COMMAND ----------

#Change Display Default

pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 200)
df_after_droping_Non_relevant_features = df_after_drop_by_Column_threshold
# Create Summary Data Frame
summary = df_after_droping_Non_relevant_features.describe(include='all')
summary.loc['dtype'] = df_after_droping_Non_relevant_features.dtypes
summary.loc['size'] = len(df_after_droping_Non_relevant_features)
summary.loc['% count null'] = df_after_droping_Non_relevant_features.isnull().mean()
summary.loc['count null'] = df_after_droping_Non_relevant_features.isnull().sum()
summary.loc['nunique'] = df_after_droping_Non_relevant_features.nunique()
summary.loc['#Zeros'] =  (df_after_droping_Non_relevant_features == 0).sum(axis=0)

#4. Transpose statistics to get similar format as R summary() function
summary = summary.transpose().sort_values(by='% count null',ascending=False)
summary

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Dealing with outliers  -/z-score    </h4>
# MAGIC <a link>https://towardsdatascience.com/the-matplotlib-line-plot-that-crushes-the-box-plot-912f8d2acd49</link>
# MAGIC <h4>Ways to Detect and Remove the Outliers<h4>

# COMMAND ----------

#outliers_count = df.select_dtypes(include=['float64','int64']) not working when there is columns in other types
df_after_droping_Non_relevant_features.reset_index()
outliers = df_after_droping_Non_relevant_features.drop(['Status', 'ATJ','Process S/N', 'I','Start_year_month','Diffrence in Minutes'], axis=1)
outliers.head()
# float64(94), int64(3)

# COMMAND ----------

#### Find columns with just 0 or 1 unique value ###
Columns_less_the_nunique_thershold_values = []
def clean_outliers (df,quantile=0.005,nunique_thershold=5):
    '''
    this function take dataframe and exclude all features that suspected to be categorial 
    or all have less the uniuque thrshold   - its can effect the results like  0,1  after outliers turn to nan

    parameters:
    df = dataframe - we should exclude all the categorial features
    quantile = depend on the sevrity
    nunique_thershold = define what is the thershold for numeric catgorial
    '''
    
    df_columns_series = df.columns
    Columns_less_the_nunique_thershold_values = list(df_columns_series[outliers.nunique()<=5])
    print(Columns_less_the_nunique_thershold_values)
    #Columns_less_the_two_unique_values
    df_after_drop_null = outliers.drop(Columns_less_the_nunique_thershold_values,axis=1)
    #print(f'total columns droped:{len(Columns_less_the_two_unique_values)}\nlist of columns Droped by unique values: {Columns_less_the_two_unique_values}\ndata frame structure after droping {df_after_drop_null.shape}')
    lb = df_after_drop_null.quantile(quantile)
    ub = df_after_drop_null.quantile((1-quantile))
    df_after_drop_outliers = df_after_drop_null[(df_after_drop_null < ub) & (df_after_drop_null > lb)] 
    df_after_drop_outliers.fillna(df_after_drop_outliers.mean(), inplace=True)
    for x in Columns_less_the_nunique_thershold_values:
        df_after_drop_outliers.loc[:,x] = outliers[x]
        df_after_drop_outliers[x].fillna(df_after_drop_outliers[x].mode()[0], inplace=True)
    return df_after_drop_outliers,Columns_less_the_nunique_thershold_values


# COMMAND ----------



df_after_drop_outliers,Columns_less_the_nunique_thershold_values = clean_outliers(outliers)
df_after_drop_outliers.shape



# COMMAND ----------

len(Columns_less_the_nunique_thershold_values)

# COMMAND ----------

df_after_drop_outliers

# COMMAND ----------


#'Digipot', 'Waiver'
df_after_drop_outliers.loc[:,'Waiver'] = outliers['Waiver']
df_after_drop_outliers['Waiver'].fillna(df_after_drop_outliers['Waiver'].mode()[0], inplace=True)
df_after_drop_outliers.loc[:,'Digipot'] = outliers['Digipot']
df_after_drop_outliers['Digipot'].fillna(df_after_drop_outliers['Digipot'].mode()[0], inplace=True)

# COMMAND ----------

x = set(df_after_drop_outliers.columns)

# COMMAND ----------

df_after_drop_outliers

# COMMAND ----------

outliers['Waiver']

# COMMAND ----------

df_after_drop_outliers

# COMMAND ----------

outliers.shape

# COMMAND ----------

df_after_drop_outliers['Min Angle'].mean()

# COMMAND ----------

create_null_chart(df_after_drop_outliers)

# COMMAND ----------



# COMMAND ----------

df_after_drop_outliers.index

# COMMAND ----------

from sklearn.preprocessing import MinMaxScaler
# define min max scaler
scaler = MinMaxScaler()
# transform data
col_names = list(df_after_drop_outliers.columns)
index = df_after_drop_outliers.index
scaled = scaler.fit_transform(df_after_drop_outliers)
mr_data = pd.DataFrame(scaled,columns = col_names,index = index)
mr_data


# COMMAND ----------

mr_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding targets after manipulation

# COMMAND ----------


mr_data.loc[:,'Status'] = df_after_droping_Non_relevant_features['Status']



# COMMAND ----------

get_target_split(mr_data['Status'])

# COMMAND ----------

xlsfilename = r'C:\BI\Python\00_ KnowledgeHub\018_ HP Projects\WH Prediction\data\processed\mr_data+.xlsx'
df.to_excel(xlsfilename,
             sheet_name='MR_Data')  

# COMMAND ----------

# MAGIC %md
# MAGIC # Export MR data without duplication to pickle

# COMMAND ----------


filename = r'C:\BI\Python\00_ KnowledgeHub\018_ HP Projects\WH Prediction\data\processed\mr_row_data_without_duplication_28_10_v1'
#filename = r'data\processed\mr_row_data_without_duplication'
#'.\..\mr_row_data_without_duplication'
outfile = open(filename,'wb')
#.\data\processed\


# COMMAND ----------

pickle.dump(mr_data, outfile) #its dumps the object as is
outfile.close()

# COMMAND ----------

infile = open(filename,'rb')
MR_DATA = pickle.load(infile)
infile.close

#print(MR_DATA)
#print(new_dict==MR_DATA)
print(type(MR_DATA))

# COMMAND ----------

# MAGIC %md
# MAGIC # Blankets lifespan

# COMMAND ----------

import pyspark.pandas as ps
df = ps.read_parquet("/mnt/Blankets_model/azureml/Blankets_model/Dev/Loading/Blankets_impact")
type(df)

# COMMAND ----------

df.head()

# COMMAND ----------

df['Consumable_SN'] = ((df['Consumable_SN'].str.strip()).str.upper()).str.slice(stop=10)
#((df['Consumable_SN'].str[:-3]

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC  %sql
# MAGIC SHOW TABLES FROM BLANKETS_DB

# COMMAND ----------

Press_Serial_Number || upper(Consumable_SN)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SN_Key,sum(max_lifespan) AS lifespan,sum(Count_ROR) AS ROR_PER_SN from (
# MAGIC SELECT SN_Key,ROR,max(Event_Lifespan) AS max_lifespan,count(distinct ROR) AS Count_ROR  From (
# MAGIC SELECT   Press_Serial_Number || upper(Consumable_SN) AS SN_Key,ROR,Event_Lifespan FROM BLANKETS_DB.Blankets_lifespan
# MAGIC --where upper(Consumable_SN) in ('NDV2457080000','KMV0651582000','NDW1704100000','NDW3499677000')
# MAGIC ) A 
# MAGIC group by SN_Key,ROR ) b
# MAGIC group by SN_Key
# MAGIC --,'NDU2299219000','KMW3633140000','KMV1116057000')
# MAGIC Having sum(max_lifespan) < 1000000
# MAGIC order by upper(lifespan) desc
# MAGIC 
# MAGIC --Group by Consumable_SN
# MAGIC --Order by Count_SN desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT   * FROM BLANKETS_DB.Blankets_lifespan
# MAGIC where upper(Consumable_SN) in ('NDV2457080000','KMV0651582000','NDW1704100000','NDW3499677000')--,'NDU2299219000','KMW3633140000','KMV1116057000')
# MAGIC order by upper(Consumable_SN),Replacement_DateTime desc
# MAGIC --Group by Consumable_SN
# MAGIC --Order by Count_SN desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC SELECT   upper(Consumable_SN) AS Consumable_SN,count (* ) AS counter ,sum(Optimized_Lifespan) AS impression FROM BLANKETS_DB.Blankets_lifespan
# MAGIC --where upper(Consumable_SN) = 'KMV3952132000'
# MAGIC group by upper(Consumable_SN)
# MAGIC --Having  count (*)<7 
# MAGIC ) A
# MAGIC order by counter desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select counter,Consumable_SN,impression from (
# MAGIC SELECT   upper(Consumable_SN) AS Consumable_SN,count (* ) AS counter ,sum(Optimized_Lifespan) AS impression FROM BLANKETS_DB.Blankets_lifespan
# MAGIC --where upper(Consumable_SN) = 'KMV3952132000'
# MAGIC group by upper(Consumable_SN)
# MAGIC  Having  count (*)=2 
# MAGIC ) A
# MAGIC --Group by counter
# MAGIC order by impression desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select counter,Count(Consumable_SN),Avg(impression) from (
# MAGIC SELECT   upper(Consumable_SN) AS Consumable_SN,count (* ) AS counter ,sum(Optimized_Lifespan) AS impression FROM BLANKETS_DB.Blankets_lifespan
# MAGIC --where upper(Consumable_SN) = 'KMV3952132000'
# MAGIC group by upper(Consumable_SN)
# MAGIC -- Having  count (*)<7 
# MAGIC ) A
# MAGIC Group by counter
# MAGIC order by counter desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YES-LAST REINSTALLED - count SN
# MAGIC SELECT   count (distinct upper(Consumable_SN) ) FROM BLANKETS_DB.Blankets_lifespan
# MAGIC where Is_Last_Replacement = 'YES-LAST REINSTALLED' -- not Relevent

# COMMAND ----------

# MAGIC %sql
# MAGIC --distinct all - count SN
# MAGIC SELECT   count (distinct upper(Consumable_SN) ) FROM BLANKETS_DB.Blankets_lifespan

# COMMAND ----------

#df_count_SN_Duplication = df['Consumable_SN'].groupby('Consumable_SN').agg(['count'])
sql = '''
SELECT   upper(Consumable_SN) as Consumable_SN , Count(*) AS Count_SN FROM BLANKETS_DB.Blankets_lifespan
Group by upper(Consumable_SN)
Order by Count_SN desc
  '''

# capture stats in dataframe 
Consuamble_test = spark.sql(sql)

# display stats
display(Consuamble_test) 
#df_count_SN_Duplication = df[['Consumable_SN']].groupby('Consumable_SN').count()
#df_count_SN_Duplication.head()

# COMMAND ----------

