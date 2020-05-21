## window
#1. 批次 time 10s
#2. 窗口size 30s
#3. slip time 20s

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os
from datetime import datetime


conf = SparkConf()\
        .setMaster('local[*]')\
        .setAppName('ysh_window')

sc = SparkContext.getOrCreate(conf)

ssc =  StreamingContext(sc,5)
ssc.checkpoint('/0603/')

dstream = ssc.socketTextStream('superman-bigdata.com': 9999)


result = dstream.flatMap(lambda line: line.split(','))\
        .map(lambda word: (word,1))\
        .reduceByKey(lambda a,b:a+b)\
        .reduceByKeyAndWindow(lambda a,b: a+b, lambda c,d:c-d,30,20)


result.pprint()

ssc.start()
ssc.awaitTermination()