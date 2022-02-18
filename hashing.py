from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession,HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
conf = SparkConf().setAppName(value="maskingDataGeneration")
sc = SparkContext(conf = conf)
spark = SparkSession(sc).builder.enableHiveSupport().getOrCreate()

p = spark.createDataFrame([(1234323,)], ['a']).select(xxhash64('a').alias('hash'))
print(p.collect())