from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

p = spark.read.csv("C:\data\concat_wsCheck.csv")
k = p.toPandas()
f = k.merge(k,k,how='left',left_on='id',right_on='id')
print(f)
