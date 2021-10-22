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
#conf = SparkConf().setAppName(value="datagenartion")
conf = SparkConf().setAppName(value="datagenartion").set('spark.scheduler.mode', 'FAIR').set("spark.scheduler.allocation.file", "C:/Users/mjoshi/PycharmProjects/pythonProject/fairscheduler.xml").set("spark.scheduler.pool", "production")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("WARN")

ls = [i for i in range(1,60)]

batches = len(ls)/4
start = 0
end = 4
emp = spark.read.csv("C:/data/emp.csv")
for i in batches:
    end = 4*(i+1)
#create method call it in threadpool. filter: on list parameters append to a single dataframe

