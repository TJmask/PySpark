from pyspark import SparkConf, SparkContext
import os, time
from pysparkWordcount import conf
from datetime import datetime

# if 'SPARK_HOME' not in os.environ:
#     os.environ['SPARK_HOME'] = '.....'
#     os.environ['PYSPARK_PYTHON'] = '.....'
# conf =  SparkConf().setMaster('local[*]').setAppName('ysh')



sc = SparkContext.getOrCreate(conf)
path = 'Datasets/RidingMowers.csv'
rdd = sc.textFile(path)
print(rdd.take(2))

## word frequency
result = rdd.flatMap(lambda line: line.split(',')).map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)

## output
# result.saveAsTextFile('Datasets/wordCount'+datetime.now().strftime('%Y-%m-%d'))

# def func(iter):
#     for i in iter:
#         print(i)
# result.foreachPartition(lambda iter:func(iter))



# def fun(iter):
#     for i in iter:
#         return i

# result.mapPartitions(lambda iter: fun(iter)).collect()





### top N
topNRDD = result.top(3, key=lambda x: x[1]) ## based on value
for i in topNRDD:
    print(i[0], i[1])


topNRDD = result.top(3) ## based on both value and key
for i in topNRDD:
    print(i[0], i[1])