from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os
from datetime import datetime


conf = SparkConf()\
        .setMaster('local[*]')\
        .setAppName('ysh_transform_foreachRDD')

sc = SparkContext.getOrCreate(conf)

ssc =  StreamingContext(sc,5)


dstream = ssc.socketTextStream('superman-bigdata.com': 9999)

def ysh(rdd):
    result = rdd.flatMap(lambda line: line.split('\t'))\
        .map(lambda word: (word,1))\
        .reduceByKey(lambda a, b:a+b)
    return result

dstream1 = dstream.transform(lambda rdd: ysh(rdd))
dstream1.pprint()



def ysh_2(rdd,time):
    result1 = rdd.flatMap(lambda line: line.split('\t'))\
        .map(lambda word: (word,1))\
        .reduceByKey(lambda a, b:a+b)\
        .map(lambda t: (time.strftime('%Y=%m-%m'), t))
    return result1

dstream2 = dstream.transform(lambda time,rdd: ysh_2(rdd,time))
dstream2.pprint()




## foreachRDD
def ysh_3(rdd):
    result = rdd.flatMap(lambda line: line.split('\t'))\
        .map(lambda word: (word,1))\
        .reduceByKey(lambda a, b:a+b)
    print(result.collect())

dstream.foreachRDD(lambda rdd: ysh_3(rdd))
