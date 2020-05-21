from pyspark import SparkConf, SparkContext
import os, time, random
from pysparkWordcount import conf
from datetime import datetime


sc = SparkContext.getOrCreate(conf)
path = 'Datasets/RidingMowers.csv'
rdd = sc.textFile(path)
# print(rdd.take(2))


## way one
## 两阶段聚合（key+随机前缀+局部聚合+全局聚合）

def topn(key, iter):
    sortedIter = sorted(iter, reverse=True)
    top3 = sortedIter[0:3]
    return map(lambda x: (key,x), top3)


###((8,a),iter(100,102,98,99))
result = rdd.map(lambda line: line.split(','))\
            .filter(lambda arr: len(arr)==2)\
            .mapPartitions(lambda iter: map(lambda arr: ((random.randint(1,10),arr[0]), int(arr[1])), iter))\
            .groupByKey()\
            .flatMap(lambda t: (t[0][1], t[1]))\
            .groupByKey()\
            .map(lambda t: (t[0], t[1]))

print(result.collect())




## way two
## using aggregateByKey
def f(a,b):
    iter = a.append(b)
    sorted_iter = sorted(iter, reverse=True)
    top3 = sorted_iter[0:3]
    return top3

def h(c,d):
    for i in d:
        iter = c.append(i)
    sorted_iter = sorted(iter, reverse=True)
    top3 = sorted_iter[0:3]
    return top3

result = rdd.map(lambda line: line.split(','))\
            .filter(lambda arr: len(arr)==2)\
            .map(lambda arr: (arr[0], arr[1]))\
            .aggregateByKey(zeroValue=[], seqFunc=lambda a,b:f(a,b), combFunc=lambda x,y:h(x,y))

print(result.collect())