from pyspark import SparkContext

from pyspark.sql import SparkSession, types


sparkSess = SparkSession.builder.appName('post_history').getOrCreate()
sc = sparkSess.sparkContext
bdschema = types.StructType([
        types.StructField('index', types.IntegerType()),
        types.StructField('id', types.IntegerType()),
        types.StructField('creation_date', types.StringType()),
        types.StructField('post_id', types.IntegerType()),
        types.StructField('post_history_type_id', types.IntegerType()),
        types.StructField('user_id', types.IntegerType())])

sbad=sparkSess.read.format("s3selectCSV").schema(bdschema).options(header="true").options(delimiter="|").options(quote='\"').load("s3://bigdata-4/post_history.csv").select("index","id","creation_date","post_id","post_history_type_id","user_id")

sbad.write.mode("append").parquet("s3://bigdata-4/post_history_new/")
