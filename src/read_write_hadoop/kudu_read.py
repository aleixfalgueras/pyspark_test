def read_kudu_table(spark, table_name):
    return spark.read \
        .format('org.apache.kudu.spark.kudu') \
        .option('kudu.master', 'host:port, host:port, host:port') \
        .option('kudu.table', f"impala::{table_name}") \
        .load()
