def print_spark_config(spark):
    print(f"""Spark session has been created with: 
            --driver-memory: {spark.sparkContext.getConf().get('spark.driver.memory')}
            --num-executors: {spark.sparkContext.getConf().get('spark.executor.instances')}
            --executor-cores: {spark.sparkContext.getConf().get('spark.executor.cores')}
            --executor-memory: {spark.sparkContext.getConf().get('spark.executor.memory')}""")

