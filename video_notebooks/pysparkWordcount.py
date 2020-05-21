from pyspark import SparkConf, SparkContext
import os

# if 'SPARK_HOME' not in os.environ:
#     os.environ['SPARK_HOME'] = '.....'
#     os.environ['PYSPARK_PYTHON'] = '.....'


conf =  SparkConf().setMaster('local[*]').setAppName('ysh')
sc = SparkContext.getOrCreate(conf)
print(sc)


# m = [5,2,0]
# rdd = sc.parallelize(m)
# print(rdd.collect())
# print(rdd.getNumPartitions())
# print(rdd.glom().collect())
