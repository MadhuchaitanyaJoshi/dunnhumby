import sys
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
conf = SparkConf().setAppName(value="datagenartion")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("WARN")
arg = sys.argv
appid = sc._jsc.sc().applicationId()
appname = sc._jsc.sc().appName()

def postgresWrite():
    pass
def hiveWrite():
    pass

def bigQuery():
    pass

try:
    with open(arg[2],"r") as mstcolumns:
        mstcols = mstcolumns.read().split(",")
    with open(arg[1],"r") as config:
        conf = json.load(config)
    for db in conf:
        for table in conf[db]:
            if table!="outdb":
                df = spark.read.format("jdbc").option("url","jdbc:mysql://localhost/{}".format(db)).option("driver","com.mysql.cj.jdbc.Driver").option("useSSL","false").option("dbtable",table).option("user","root").option("password","root").load()
                sampledf = df.sample(conf[db][table]["fraction"])
                while(sampledf.count()==0 or sampledf.count()>(df.count()*conf[db][table]["fraction"])):
                    sampledf = df.sample(conf[db][table]["fraction"])
                cols = df.dtypes
                for colm in cols:
                    basecol = conf[db][table]["maskcolumns"]
                    if(colm[0] in mstcols and colm[1]=="int"):
                        sampledf = sampledf.withColumn(conf[db][table]["maskcolumns"][colm[0]], when(hash(sampledf[colm[0]])<0,hash(sampledf[colm[0]])*-1 ).otherwise(hash(sampledf[colm[0]])))
                    if(colm[0] in mstcols and colm[1]=="string"):
                        sampledf = sampledf.withColumn(basecol[colm[0]], sha2((sampledf[colm[0]]), 256))
                sampledf.write.format('jdbc').options(
                    url='jdbc:mysql://localhost/{}'.format(conf[db]["outdb"]),
                    driver='com.mysql.cj.jdbc.Driver',
                    dbtable='{}'.format(conf[db][table]["outtable"]),
                    user='root',
                    password='root').option("useSSL","false").mode('append').save()
except Exception as e:
    print(e)
    print(appid, appname)
    errorTime = spark.sql(
        "select from_unixtime(unix_timestamp(current_timestamp,'yyyy-MM-dd HH:mm:ss'),'dd-MM-yyyy h:mm:ss') s").collect()[
        0]['s']
    errorList = sc.parallelize([(appid, appname, errorTime, "error")], 1)
    schema = StructType([
        StructField("appid", StringType(), True),
        StructField("appname", StringType(), True),
        StructField("errortime", StringType(), True),
        StructField("description", StringType(), True)])
    errordf = spark.createDataFrame(errorList, schema=schema)
    errordf = errordf.withColumn("errortime", to_timestamp(errordf.errortime, 'MM-dd-yyyy HH:mm:ss'))
    postgresWrite()
