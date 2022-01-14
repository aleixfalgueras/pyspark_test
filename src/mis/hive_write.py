from pyspark.sql import SparkSession


# DataFrameWriter: https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-DataFrameWriter.html#saveAsTable-internals
# Offical docu: https://spark.apache.org/docs/2.4.0/api/java/org/apache/spark/sql/DataFrameWriter.html#saveAsTable-java.lang.String-

# Start point
spark = SparkSession.builder.appName("Pyspark Aleix")\
    .master("local") \
    .config("spark.driver.memory", "4G") \
    .config("spark.executor.memory", "4G") \
    .config("spark.executor.instances", "1") \
    .config("spark.executor.cores", "1") \
    .enableHiveSupport() \
    .getOrCreate()

""" If your Spark application is interacting with Hadoop, Hive, or both use spark hadoop properties in the 
form of spark.hadoop.* in the conf 
ex: .config("spark.hadoop.hive.exec.dynamic.partition", "true")
"""

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")

spark.conf.set("hive.exec.dynamic.partition", "true")  # by default is already True
""" 
Dynamic partitions are the partition columns that have no values defined explicitly in the 
PARTITION clause of INSERT OVERWRITE TABLE SQL statements  

Static partitions are the partition columns that have values defined explicitly in the 
PARTITION clause of INSERT OVERWRITE TABLE SQL statements
"""

spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
""" In strict mode, the user must specify at least one static partition in case 
the user accidentally overwrites all partitions """

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
""" When the dynamic overwrite mode is enabled Spark will only delete the partitions 
for which it has data to be written to. All the other partitions remain intact. """

data1 = [("1", "aleix", "20"), ("2", "jofre", "25"), ("3", "xala", "30")]
data2 = [("4", "oriol", "35")]

data3 = [("5", "40", "ruff"), ("5", "41", "ruff"), ("6", "50", "pol")]
data4 = [("7", "60", "alex"), ("7", "61", "alex"), ("7", "62", "alex"), ("6", "51", "pol")]

schema = ['pos', 'name', 'age']
schema_partitioned = ['pos', 'age', 'name']

df1 = spark.createDataFrame(data1, schema)
df2 = spark.createDataFrame(data2, schema)

df3 = spark.createDataFrame(data3, schema_partitioned)
df4 = spark.createDataFrame(data4, schema_partitioned)

# targe tables name
target_table = "ds_hamil_up.aleix_hive_write"
target_table_partitioned = "ds_hamil_up.aleix_hive_write_partitioned"
target_table_partitioned_at_fly = "ds_hamil_up.aleix_hive_write_partitioned_at_fly"

### CREATE TABLES ###

spark.sql(f"DROP TABLE IF EXISTS {target_table} PURGE")
spark.sql(f"DROP TABLE IF EXISTS {target_table_partitioned} PURGE")
spark.sql(f"DROP TABLE IF EXISTS {target_table_partitioned_at_fly} PURGE")

# "location" is required in the create script if the table is external

spark.sql(f"""CREATE TABLE IF NOT EXISTS {target_table} (
    pos STRING,
    name STRING,
    age STRING)
stored as parquet""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {target_table_partitioned} (
    pos STRING,
    age STRING)
partitioned by (name STRING)
stored as parquet""")

### NOT PARTITIONED WRITES ###

# overwrite writes
df1.write.mode("overwrite").saveAsTable(target_table)  # column-name based resolution (best create strategy)
df2.write.mode("overwrite").insertInto(target_table, overwrite=True)  # position-based resolution
# result: 1 directory 1 file

# append writes
df1.write.mode("append").saveAsTable(target_table)
df2.write.mode("append").insertInto(target_table, overwrite=False)
# result: 1 directory, multiple files

### PARTITIONED WRITES ###

""" last value/s must be the partition/s column/s"""
df3.write.mode("overwrite").insertInto(target_table_partitioned, overwrite=True)
df4.write.mode("append").insertInto(target_table_partitioned, overwrite=False)

"""
partitionBy() indicates the partition data columns
maxRecordsPerFile is particulary useful when data is skew
use repartition/coalesce before the ".write" to determine the number of files PER partition
source: https://sparkbyexamples.com/pyspark/pyspark-partitionby-example/
"""
df3.union(df4)\
    .write \
    .option("maxRecordsPerFile", 2) \
    .partitionBy('name')\
    .mode("overwrite")\
    .saveAsTable(target_table_partitioned_at_fly)

