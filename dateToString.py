import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
sc = SparkContext()
spark = SparkSession(sc)
row = Row("vacationdate")

df = sc.parallelize([
    row(datetime.date(2015, 10, 07)),
    row(datetime.date(1971, 01, 01))
]).toDF()