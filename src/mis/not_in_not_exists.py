import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

findspark.init()
spark = SparkSession.builder.master("local").appName("Explode and pivot demostration").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = [('1', 1), ('2', 2), ('3', 3), ('3', 5)]
data_null = [('1', 1), ('2', 2), (None, 999), ('4', 4)]

df = spark.createDataFrame(data, ['id', 'age'])
df_null = spark.createDataFrame(data_null, ['id', 'age'])
df_null_2 = spark.createDataFrame(data_null, ['id', 'age'])

df.createOrReplaceTempView("df")
df_null.createOrReplaceTempView("df_null")
df_null_2.createOrReplaceTempView("df_null_2")

# source: https://docs.microsoft.com/en-us/azure/databricks/kb/_static/notebooks/sql/broadcastnestedloopjoin-example.html

spark.sql("select id from df where id not in (select id from df_null)").show()
""" Result: null values in the 'not in' table referenced causes an empty result!
+---+
| id|
+---+
+---+   

Due to this behaviour, in order to know all the values and ensure that there's no null value Spark does a BroadcastNestedLoopJoin, 
even when spark.sql.autoBroadcastJoinThreshold is set to -1 (disabled). This can cause memory issues.
"""

spark.sql("select id from df_null where id not in (select id from df)").show()
""" Result: null values of the source table are ignored in the result set final
+---+
| id|
+---+
|  4|
+---+
"""

spark.sql("select * from df where age = 3 and not exists (select 1 from df_null where df.id = df_null.id)").show()
""" Result: unlike 'not in', the 'not exists' does not care about null values in the table referenced
+---+---+
| id|age|
+---+---+
|  3|  3|
+---+---+
"""

spark.sql("select * from df_null where not exists (select 1 from df where df_null.id = df.id)").show()
spark.sql("select * from df_null where not exists (select 1 from df_null_2 where df_null.id = df_null_2.id)").show()
""" Result: null values from the source table are returned, even if there are null values in the table referenced (df_null_2)
+----+---+
|  id|age|
+----+---+
|null|999|
|   4|  4|
+----+---+

+----+---+
|  id|age|
+----+---+
|null|999|
+----+---+
"""