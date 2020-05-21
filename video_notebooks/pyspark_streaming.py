from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os


conf = SparkConf()\
        .setMaster('local[*]')\
        .setAppName('ysh_streaming')

sc = SparkContext.getOrCreate(conf)

ssc =  StreamingContext(sc,5)


dstream = ssc.socketTextStream('superman-bigdata.com': 9999)

result = dstream.flatMap(lambda line: line.split(','))\
        .map(lambda word: (word,1))\
        .reduceByKey(lambda a,b:a+b)

result.pprint()

ssc.start()
ssc.awaitTermination()