import concurrent.futures as f
import asyncio
import time
from functionFile import postgresWriteTable
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import math

sc = SparkContext()
spark = SparkSession(sc)

rd = sc.parallelize([(12,),(-10,),(-19,),(30)])
df = spark.createDataFrame(rd,["num"])

print(df.show())