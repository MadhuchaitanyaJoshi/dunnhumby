from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
conf = SparkConf().setAppName("postgres")
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
sc.setLogLevel("WARN")
from datetime import datetime
import psycopg2

appid = sc._jsc.sc().applicationId()
appname = sc._jsc.sc().appName()
print(appid,appname)
errorTime = datetime.now().timestamp()
#print(k.show())
startTime = spark.sql("select from_unixtime(unix_timestamp(current_timestamp,'yyyy-MM-dd HH:mm:ss'),'dd-MM-yyyy h:mm:ss') s").collect()[0]['s']
errorList = sc.parallelize([(appid,appname,startTime,"error")],1)
print(errorList)

print(type(startTime))
schema = StructType( [
                 StructField("appid", StringType(),True),
                 StructField("appname", StringType(),True),
                 StructField("errortime", StringType(),True),
                 StructField("description", StringType(),True)])
errordf = spark.createDataFrame(errorList,schema=schema)
errordf = errordf.withColumn("errortime",to_timestamp(errordf.errortime,'MM-dd-yyyy HH:mm:ss'))
print(errordf.show())
print(errordf.printSchema())
url_connect = "jdbc:postgresql://localhost:5432/retail"
table = "cust"
mode = "append"
properties = {"user": "postgres","password": "postgres","driver": "org.postgresql.Driver"}
errordf.write.option('driver', 'org.postgresql.Driver').jdbc(url_connect, table, mode, properties)
