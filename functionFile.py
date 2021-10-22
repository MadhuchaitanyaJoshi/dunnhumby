def hiveReadTable(spark,in_db,in_table):
    df = spark.sql("select * from {}.{}".format(in_db,in_table))
    return df

def postgresReadTable(spark,in_url_connect, in_user, in_password, in_driver, in_table):
    properties = {"user": in_user, "password": in_password, "driver": in_driver}
    df = spark.read.jdbc(url=in_url_connect, table=in_table, properties=properties)
    return df


def bigqueryReadTable(spark,in_table,in_project):
    df = spark.read.format('bigquery') \
        .option('project', in_project)\
        .option('table', in_table) \
        .load()
    return df


def hiveWriteTable(spark,sampledf,out_db,out_table,out_mode):
    sampledf.write.mode(out_mode).saveAsTable("{}.{}".format(out_db,out_table))


def postgresWriteTable(spark,sampledf,out_url_connect, out_user, out_password, out_driver,out_mode,out_table):
    properties = {"user": out_user, "password": out_password, "driver": out_driver}
    print("\n",sampledf,out_url_connect, out_user, out_password, out_driver,out_mode,out_table)
    #sampledf.write.option('driver', out_driver).jdbc(out_url_connect, out_table, out_mode, properties).save()
    sampledf.show()

def bigqueryWriteTable(spark,sampledf,out_table):
    sampledf.write.format('bigquery') \
        .option('table', out_table) \
        .save()
