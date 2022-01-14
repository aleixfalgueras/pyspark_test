from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Pyspark Aleix") \
    .master("local") \
    .enableHiveSupport() \
    .config("spark.dynamicAllocation.enabled", "True") \
    .getOrCreate()

spark_session_with_executors = SparkSession.builder.appName("Pyspark Aleix") \
    .master("local") \
    .config("spark.driver.memory", "4G") \
    .config("spark.executor.memory", "4G") \
    .config("spark.executor.instances", "1") \
    .config("spark.executor.cores", "1") \
    .enableHiveSupport() \
    .getOrCreate()

"""
spark.dynamicAllocation.enabled (default: False) -> allow up and down executors instances based on the workload
"""

""" .enableHiveSupport() 
spark.sql.catalogImplementation is set to hive, otherwise to in-memory.
Provides HiveContext functions, so you're able to use catalog functions i. e. Hive user-defined functions.
"""

for con in spark_session_with_executors.sparkContext.getConf().getAll():
    print(con[0], con[1])

# spark catalog
for db in spark_session_with_executors.catalog.listDatabases():
    print(db.name)

for table in spark_session_with_executors.catalog.listTables():
    print(table.name)
