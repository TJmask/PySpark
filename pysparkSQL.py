from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .master('local[*]')\
                    .appName('ysh_sql')\
                    .getOrCreate()


## read dataframe

df = spark.read.json()