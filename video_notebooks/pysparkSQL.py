from pyspark.sql import SparkSession

# spark = SparkSession.builder\
#                     .master('local[*]')\
#                     .appName('ysh_sql')\
#                     .getOrCreate()


# ## read dataframe
# path = 'Datasets/example_1.json'
# df = spark.read.json(path, multiLine=True)
# df.show()

# # ## DSL 
# # df.printSchema()
# # df.select('color').show()
# # df.select(df['color'].alias('color1'), df['size']+'1').toDF('c1','s1').show()
# # df.filter(df['color']=='Red').show()
# # # df.groupBy('age').count().show()
# # # df.agg({'age':'avg'}).show()


# # ## temporary table
# # df.registerTempTable('json')
# # df.createTempView('e1')

# # spark.sql('select * from e1').show()

# df.createOrReplaceTempView('person')


# # ## global 
# df.createGlobalTempView('person2')

# # spark.sql('select * from global_temp.person2').show()



# ## 
# spark1 = spark.newSession()
# spark1.sql('select * from global_temp.person2').show()
# spark1.sql('select * from person').show()



## RDD to dataframe
from pyspark import SparkContext
sc = SparkContext()
rdd = sc.textFile('file:///Users/tjmask/Desktop/test/test.txt')\
        .map(lambda line: line.split(','))\
        .map(lambda arr: (arr[0],arr[1]))

print(rdd.collect())

df = rdd.toDF()
df.show()

df1 = rdd.toDF(['name', 'age]')
df1.show()


## way 2
from pyspark import Row

spark = SparkSession.builder\
                    .master('local[*]')\
                    .appName('ysh_sql')\
                    .getOrCreate()

rdd1 = sc.textFile('file:///Users/tjmask/Desktop/test/test.txt')\
        .map(lambda line: line.split(','))\


people = rdd1.map(lambda arr: Row[name=arr[0], age=arr[1]])

df2 = spark.createDataFrame(people)
df2.show()


## 
from pyspark.sql.types import *
person = rdd1.map(lambda arr: (arr[0], arr[1]))

schema = StructType([StructField('name', StringType()),
                    StructField('age', StringType()) ])



df3  = spark.createDataFrame(person, schema)
df4 = spark.read.schema(schema).json(path)


## write to Mysql
url = 'jdbc:mysql://tjmask.com:8080/ysh1314'
table = 'person'
mode = 'overwrite'
properties = {'user': 'root', 'password':'123456'}
df3.write.jdbc(url=,mode=None, properties=None )