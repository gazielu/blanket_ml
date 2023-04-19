# Databricks notebook source
secret=dbutils.secrets.get(scope="olympus-physical-movements-scope", key="secret")

# COMMAND ----------

# All Installations
dbutils.library.installPyPI('azure-keyvault-secrets','4.2.0')
dbutils.library.installPyPI('azure-identity','1.4.1') 
dbutils.library.installPyPI('pycrypto','2.6.1') 
dbutils.library.installPyPI('pyodbc','4.0.30')

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get install msodbcsql17

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

# MAGIC %sh
# MAGIC ps aux | grep -i apt

# COMMAND ----------

#all import
import uuid
import json
import pyodbc
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.identity import ClientSecretCredential
from pyspark.sql import SparkSession

# COMMAND ----------

# %sql 
# --Please change the JAR path as per the Cluster FileStore path installed 
# --eg:dbfs:/FileStore/jars/2fc5fd3e_b1a8_425f_9bec_888f5489f067-Olympus_Encrypt_Decrypt.jar

# CREATE OR REPLACE FUNCTION encrypt AS 'encryptUDFPackage.EncryptUDF' using JAR 'dbfs:/FileStore/jars/bf23306e_a13e_45f9_9883_c449f30c2221-Olympus_Encrypt_Decrypt.jar'

# COMMAND ----------

# %sql
# --Please change the JAR path as per the Cluster FileStore path installed.
# --eg:dbfs:/FileStore/jars/2fc5fd3e_b1a8_425f_9bec_888f5489f067-Olympus_Encrypt_Decrypt.jar
# --dbfs:/FileStore/jars/73aed3eb_ebad_4c6b_abf9_716760889b08-Olympus_Encrypt_Decrypt.jar

# CREATE OR REPLACE FUNCTION decrypt AS 'decryptUDFPackage.DecryptUDF'using JAR 'dbfs:/FileStore/jars/bf23306e_a13e_45f9_9883_c449f30c2221-Olympus_Encrypt_Decrypt.jar'

# COMMAND ----------

# # Get Parameter Input from ADF
# dbutils.widgets.text("JOB_ID", "","")
# dbutils.widgets.get("JOB_ID")
# JOB_ID  = getArgument("JOB_ID")
# print(JOB_ID)
# jobidlist=json.loads(JOB_ID)
# print(jobidlist)
# dbutils.widgets.text("BOW_ID", "","")
# dbutils.widgets.get("BOW_ID")
# BOW_ID  = getArgument("BOW_ID")
# print(BOW_ID)
# dbutils.widgets.text("TENANT_ID", "","")
# dbutils.widgets.get("TENANT_ID")
# TENANT_ID  = getArgument("TENANT_ID")
# #ST Added
# TENANT_ID="4"
# print(TENANT_ID)


# COMMAND ----------

#Common Spark Settings
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")

# COMMAND ----------

server = 'Server=gvs72069.inc.hpicorp.net'
database = 'GAPI-I'
username = 'GABI_STG_RW_1'
password = 'fT7_E*4U6k_m'
driver= '{ODBC Driver 17 for SQL Server}'
conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=2048;DATABASE='+database+';UID='+username+';PWD='+ password)

# COMMAND ----------

def gettingstatusflagforrestartability(job_id):
    server = 'ebdev-ebi-adbsvr-infrastruct.database.windows.net'
    database = 'ebdev-ebi-adb-infrastructure'
    username = 'TERA4Execute'
    password = dbutils.secrets.get(scope="olympus-data-engineering-keyvault-scope", key="ebi-adb-infrastructure-Execute-key")
    driver= '{ODBC Driver 17 for SQL Server}'
    conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)

    #sp
    job_id = int(job_id)
    cursor = conn.cursor()
    execsp = "EXEC [CNTRL].[USP_JOB_ORCHESTRATION_CONTROL_COMMON_RESTARTABILITY] {0},{1},R".format(job_id,TENANT_ID)
    print(execsp)
    conn.autocommit = True
    cursor.execute(execsp)
    status_flag=cursor.fetchall()
    for row in status_flag:
        status = row[0]
        print("status: ", status)
        return status

# COMMAND ----------

#performing the operation (calling Child Notebook)
retry_count = 0
for value in jobidlist.values():
    value=value.replace('[',"")
    value=value.replace(']',"")
    for job_id_list in value.split(","):        
        print(job_id_list, type(job_id_list))
        batch_exec = uuid.uuid4().hex
        from main.JobMultiThreadingStartup import JobMultiThreadingStartup
        spark_config = spark.sparkContext.getConf().getAll()
        if __name__ == "__main__":
            print('inside main')
            obj = JobMultiThreadingStartup({'SPARK_CONFIG':spark, 'DBUTILS':dbutils,'ENV_TYPE':'DEV',
                               'TENANT_ID':TENANT_ID,
                               'JOB_ID_LIST':[job_id_list],
                               'EXECUTION_MODE':'N',
                                   'DEPENDENCY_FLAG':'R','BATCH_EXECUTION_ID':batch_exec})
        status_flag=gettingstatusflagforrestartability(job_id_list)
        print(status_flag)
            
        if status_flag=='F':
            print("Enter restartability****************")
            if retry_count <= 1:
                retry_count = retry_count + 1
                from main.JobMultiThreadingStartup import JobMultiThreadingStartup
                spark_config = spark.sparkContext.getConf().getAll()
                if __name__ == "__main__":
                    print("restartability running *******************")
                    obj = JobMultiThreadingStartup({'SPARK_CONFIG':spark, 'DBUTILS':dbutils,'ENV_TYPE':'DEV',
                               'TENANT_ID':TENANT_ID,
                               'JOB_ID_LIST':[job_id_list],
                               'EXECUTION_MODE':'R',
                                   'DEPENDENCY_FLAG':'R','BATCH_EXECUTION_ID':batch_exec})
                else:
                    pass
        else :
            print('job completed succfully')


# COMMAND ----------

