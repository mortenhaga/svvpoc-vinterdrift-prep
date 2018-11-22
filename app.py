
# Setup storage account key etc
import os
from os import environ

storage_account_name = "svvpocdlgen2"
storage_account_access_key = environ.get("AZURE_STORAGE_ACCESS_KEY").strip()


# Read all files in blob container
from azure.storage.blob import BlockBlobService

block_blob_service = BlockBlobService(account_name=storage_account_name, account_key=storage_account_access_key)
generator = block_blob_service.list_blobs('vinterdatavictortest')
filenames = []
processed = []

for blob in generator:
    if blob.name.startswith("processed_"):
        processed.append( blob.name.replace("processed_", "") )
    elif blob.name.endswith(".csv"):
        filenames.append(blob.name)

# delete the files that has already been processed
for p in processed:
    if p in filenames:
        filenames.remove(p)

print("Already processed:")
for name in processed:
    print(name)
    
print("Not processed:")
for name in filenames:
    print(name)

# If there is nothing to process. Exit.
import sys
if not filenames:
    print("Nothing new to process... Exiting....")
    sys.exit()

# Create spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('vinter-prep-wrangler').config("spark.hadoop.fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem").config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem").config("fs.azure.account.key."+storage_account_name+".blob.core.windows.net", storage_account_access_key).getOrCreate()


# DB Setup
jdbcHostname = environ.get("AZURE_SQL_HOST")
jdbcDatabase = environ.get("AZURE_SQL_DB")
jdbcPort = environ.get("AZURE_SQL_PORT")
username = environ.get("AZURE_SQL_UNAME")
password = environ.get("AZURE_SQL_PASSWD").strip()

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : username,
  "password" : password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read CSV and writo to DB
filenamesToUpdate = []
for file in filenames:
    print("Processing file: " + file)
    df = spark.read.format("csv").options(header='true',inferschema='true',sep=",").load("wasbs://vinterdatavictortest@svvpocdlgen2.blob.core.windows.net/" + file)
    print(file + " has " + str(df.count()) + " rows.")
    
    df = df.withColumn('time', to_timestamp(from_unixtime(substring(col('time').cast(StringType()), 0, 10)), 'yyyy-MM-dd HH:mm:ss'))

    # write to db
    df.write.jdbc(url=jdbcUrl, table="vinterdataOpenshift", mode="append", properties=connectionProperties)
    
    filenamesToUpdate.append("processed_" + file)



# Create files for processed files
for file in filenamesToUpdate:
    print("Will create new file: " + file)
    block_blob_service.create_blob_from_text('vinterdatavictortest', file, 'dummy')


# Stop spark
spark.stop()

