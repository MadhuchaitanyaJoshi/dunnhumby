from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
sc = SparkContext()
spark = SparkSession(sc)

df = spark.createDataFrame(spark.sparkContext.emptyRDD(),StructType([]))
print(df.show())