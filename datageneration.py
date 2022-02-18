import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
sc = SparkContext()
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("WARN")
arg = sys.argv
try:
    with open("C:/Users/mjoshi/PycharmProjects/pythonProject/masterColumns2.txt","r") as mstcolumns:
        mstcols = mstcolumns.read().split(",")
    with open("C:/Users/mjoshi/PycharmProjects/pythonProject/config2.json","r") as config:
        conf = json.load(config)
        # print(conf["retail"]["cust"])
    for db in conf:
        # print(db)
        for table in conf[db]:
            if table!="outdb":
                df = spark.read.format("jdbc").option("url","jdbc:mysql://localhost/{}".format(db)).option("driver","com.mysql.cj.jdbc.Driver").option("useSSL","false").option("dbtable",table).option("user","root").option("password","root").load()
                cols = df.dtypes
                for colm in cols:
                    basecol = conf[db][table]["maskcolumns"]
                    # print("************",basecol,colm)
                    if(colm[0] in mstcols and colm[1]=="int"):
                        # print("----------->",basecol[colm[0]])
                        df = df.withColumn(conf[db][table]["maskcolumns"][colm[0]], when(hash(df[colm[0]])<0,hash(df[colm[0]])*-1 ).otherwise(hash(df[colm[0]])))
                    if(colm[0] in mstcols and colm[1]=="string"):
                        df = df.withColumn(basecol[colm[0]], sha2((df[colm[0]]), 256))
                sampledf = df.sample(conf[db][table]["fraction"])
                # print(df.show(10,False),df.printSchema())
                # print(sampledf.show(20,False))
                # print(conf[db]["outdb"])
                while(sampledf.count()==0 or sampledf.count()>(df.count()*conf[db][table]["fraction"])):
                    sampledf = df.sample(conf[db][table]["fraction"])
                print("------------>",sampledf.show(20,False))
                sampledf.write.format('jdbc').options(
                    url='jdbc:mysql://localhost/{}'.format(conf[db]["outdb"]),
                    driver='com.mysql.cj.jdbc.Driver',
                    dbtable='{}'.format(conf[db][table]["outtable"]),
                    user='root',
                    password='root').option("useSSL","false").mode('append').save()
except Exception as e:
    print(e)