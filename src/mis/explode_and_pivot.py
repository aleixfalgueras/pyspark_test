import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

findspark.init()
spark = SparkSession.builder.master("local").appName("Explode and pivot demostration").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ****** EXPLODE ****** #

data_explode = [
    ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
    ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
    ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
    ('Washington', None, None),
    ('Jefferson', ['1', '2'], {})]

df = spark.createDataFrame(data_explode, ['name', 'lan', 'prop'])

df.select('name', explode('lan').alias('explode_lan')).show()
df.select('name', explode_outer('lan').alias('explode_lan_nulls')).show()

# null values are shown
df.select('name', posexplode('lan').alias('pos', 'explode_lan')).show()

# (null, null) if map/array is null
df.select('name', posexplode_outer('lan').alias('pos_null', 'explode_lan_null')).show()

df.select('name', explode('prop').alias('explode_prop_key', 'explode_prop_value')).show()
df.select('name', explode_outer('prop').alias('explode_prop_key_nulls', 'explode_prop_value_nulls')).show()

# ****** PIVOT ****** #

data_pivot = [("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
              ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
              ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
              ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico")]

df = spark.createDataFrame(data_pivot, ['product', 'amount', 'country'])

df.groupby('product').pivot('country').sum('amount').show()

# you can specify the new columns
pivotDF = df.groupby('product').pivot('country', ['USA', 'China']).sum('amount')
pivotDF.show()

unpivotExpr = "stack(2, 'USA', Usa, 'China', China) as (Country,Total)"
pivotDF.select('product', expr(unpivotExpr)).show()
