from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .master('local[*]')\
                    .appName('ysh_sql_hive')\
                    .getOrCreate()



### sparksql connect with hive
# 1. hive-site.xml and mysql驱动包， put them in Spark_home
# 2. open metastore


spark.sql('select * from default.cc').show()