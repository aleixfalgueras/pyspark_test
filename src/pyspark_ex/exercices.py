import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import hashlib

findspark.init()

ORDER_ID = 'order_id'
PRODUCT_ID = 'product_id'
SELLER_ID = 'seller_id'
NUM_PIECES_SOLD = 'num_pieces_sold'
BILL_RAW_TEXT = 'bill_raw_text'
DATE = 'date'
PRODUCT_NAME = 'product_name'
PRICE = 'price'
SELLER_NAME = 'seller_name'
DAILY_TARGET = 'daily_target'

sales_schema = StructType([StructField(ORDER_ID, IntegerType(), True),
                           StructField(PRODUCT_ID, IntegerType(), True),
                           StructField(SELLER_ID, IntegerType(), True),
                           StructField(DATE, DateType(), True),
                           StructField(NUM_PIECES_SOLD, IntegerType(), True),
                           StructField(BILL_RAW_TEXT, StringType(), True)])

product_schema = StructType([StructField(PRODUCT_ID, IntegerType(), True),
                             StructField(PRODUCT_NAME, StringType(), True),
                             StructField(PRICE, IntegerType(), True)])

seller_schema = StructType([StructField(SELLER_ID, IntegerType(), True),
                            StructField(SELLER_NAME, StringType(), True),
                            StructField(DAILY_TARGET, IntegerType(), True)])

#######################################################

"""
spark12 = SparkSession.builder.master("local").config("spark.sql.autoBroadcastJoinThreshold", -1) \
             .config("spark.executor.memory", "500mb").appName("Exercise1").getOrCreate()
             
spark3 = SparkSession.builder.master("local").config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "3gb").appName("Exercise1").getOrCreate()
"""

spark = SparkSession.builder.master("local").config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "3gb").appName("Exercise1").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

sales = spark.read.option('header', 'true').csv("./data/sales.csv")
products = spark.read.option('header', 'true').csv("./data/products.csv")
sellers = spark.read.schema(seller_schema).csv("./data/sellers.csv")

"""  WARM-UP 1
Find out how many orders, how many products and how many sellers are in the data.
How many products have been sold at least once? Which is the product contained in more orders?
"""

# print(f"Sales, products and sellers: {sales.count()}, {products.count()}, {sellers.count()} ")

# print("How many products have been sold at least once? " + str(sales.select('product_id').distinct().count()))
# sales.agg(countDistinct(PRODUCT_ID)).show()
#
# print("Which is the product contained in more orders?")
# sales.groupby(PRODUCT_ID).count().show()


""" WARM UP 2
How many distinct products have been sold in each day?
"""

# spark = SparkSession.builder.master("local").config("spark.sql.autoBroadcastJoinThreshold", -1) \
#         .config("spark.executor.memory", "500mb").appName("Exercise1").getOrCreate()
#
# print("How many distinct products have been sold in each day?")
# sales.groupby(DATE).agg(countDistinct(PRODUCT_ID))

""" EXERCICE 1
What is the average revenue of the orders?
"""

# sales\
#     .join(products, [PRODUCT_ID])\
#     .agg(avg(col(PRICE) * col(NUM_PIECES_SOLD)))\
#     .show()

""" EXERCICE 2
For each seller, what is the average % contribution of an order to the seller's daily quota?
"""

# CONTRIBUTION = 'contribution'
#
# sales\
#     .join(broadcast(sellers.select(SELLER_ID, DAILY_TARGET)), [SELLER_ID])\
#     .withColumn(CONTRIBUTION, col(NUM_PIECES_SOLD) / col(DAILY_TARGET))\
#     .groupby(SELLER_ID)\
#     .avg(CONTRIBUTION)\
#     .show()

""" EXERCICE 3
Who are the second most selling and the least selling persons (sellers) for each product? 
Who are those for product with 'product_id = 0'
"""

RANK = 'rank'

window_rank = Window.partitionBy(PRODUCT_ID).orderBy(desc(col(NUM_PIECES_SOLD)))

sales_res = sales\
    .withColumn(RANK, dense_rank().over(window_rank)) \
    .dropDuplicates([PRODUCT_ID, RANK])

sales_res.cache()

sales_second = sales_res.filter(col(RANK) == lit(2))\

window_last = Window.partitionBy(PRODUCT_ID)

sales_last = sales_res\
    .withColumn('HIT', when(max(col(RANK)).over(window_last) == col(RANK), lit('HIT')).otherwise(lit('')))\
    .filter(col('HIT') == lit('HIT'))

sales_second\
    .union(sales_last.drop('HIT'))\
    .select(ORDER_ID, PRODUCT_ID, SELLER_ID, NUM_PIECES_SOLD)\
    .orderBy(PRODUCT_ID)\
    .show()

""" EXERCICE 4
Create a new column called "hashed_bill" defined as follows:
- if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') 
  present in the text. 
  
  Ex: if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)

- if the order_id is odd: apply SHA256 hashing to the bill text

Finally, check if there are any duplicate on the new column
"""

HASHED_BILL = 'hashed_bill'


def hash_it(order_id, bill_text):
    ret = bill_text.encode('utf-8')
    if int(order_id) % 2 == 0:
        count_a = bill_text.count('A')
        for _c in range(0, count_a):
            ret = hashlib.md5(ret).hexdigest().encode('utf-8')
        ret = ret.decode('utf-8')
    else:
        ret = hashlib.sha256(ret).hexdigest()
    return ret


hash_it_udf = spark.udf.register("hash_it", hash_it)

sales.withColumn(HASHED_BILL, hash_it_udf(col(ORDER_ID), col(BILL_RAW_TEXT))).show()

#######################################################

input()
