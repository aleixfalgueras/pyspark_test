from pyspark.sql import SparkSession


def create_spark_session_standalone(executors=2,
                                    executor_cores=4,
                                    executor_memory=8,
                                    driver_memory=4,
                                    app_name="PySpark Aleix"):
    spark = SparkSession.builder.appName(app_name) \
        .master("local") \
        .config("spark.driver.memory", f"{driver_memory}G") \
        .config("spark.executor.memory", f"{executor_memory}G") \
        .config("spark.executor.instances", executors) \
        .config("spark.executor.cores", executor_cores) \
        .enableHiveSupport() \
        .config("spark.sql.broadcastTimeout", "36000") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.hadoop.hive.exec.max.dynamic.partitions", "100000") \
        .config("spark.hadoop.hive.exec.max.dynamic.partitions.pernode", "100000") \
        .getOrCreate()

    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")

    return spark


def print_spark_config(spark):
    print(f"""Spark session has been created with: 
            --driver-memory: {spark.sparkContext.getConf().get('spark.driver.memory')}
            --num-executors: {spark.sparkContext.getConf().get('spark.executor.instances')}
            --executor-cores: {spark.sparkContext.getConf().get('spark.executor.cores')}
            --executor-memory: {spark.sparkContext.getConf().get('spark.executor.memory')}""")

