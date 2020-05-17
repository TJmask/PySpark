from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os
from datetime import datetime


conf = SparkConf()\
        .setMaster('local[*]')\
        .setAppName('ysh_updateByKey')

sc = SparkContext.getOrCreate(conf)

ssc =  StreamingContext(sc,5)
ssc.checkpoint('/0603/')

dstream = ssc.socketTextStream('superman-bigdata.com': 9999)

def updateFunc(currentValue, prestate):
    if prestate is None:
        prestate=0
    return sum(currentValue+prestate)

result = dstream.flatMap(lambda line: line.split(','))\
        .map(lambda word: (word,1))\
        .reduceByKey(lambda a,b:a+b)\
        .updateStateByKey(updateFunc=)


result.pprint()

ssc.start()
ssc.awaitTermination()



