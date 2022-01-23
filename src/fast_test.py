import findspark
import utils
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

findspark.init()
spark = SparkSession.builder.master("local").appName("FAST TEST").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
utils.print_spark_config(spark)

df_test = spark.createDataFrame([(1, "value1"), (2, "value2")], ["id", "value"])
# read from data -> spark.read.text('../data/friends.txt').show()

#######################################################

data = [("James", "", "Smith", "36636", "M", 0.8),
        ("James", "Rose", "", "40288", "M", 1.2),
        ("Robert", "", "Williams", "42114", "M", 1.25),
        ("Maria", "Anne", "Jones", "39192", "F", 0.7),
        ("Jen", "Mary", "Brown", "", "F", 0.9)
        ]

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("float_col", FloatType(), True)
])

df_articles = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

df_articles.show()


#######################################################

# input()
