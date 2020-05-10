from pyspark import SparkContext

from pyspark.sql import SparkSession, types


sparkSess = SparkSession.builder.appName('users').getOrCreate()
sc = sparkSess.sparkContext
bdschema = types.StructType([
        types.StructField('index', types.IntegerType()),
        types.StructField('id', types.IntegerType()),
        types.StructField('age', types.IntegerType()),
        types.StructField('creation_date', types.StringType()),
        types.StructField('last_access_date', types.StringType()),
        types.StructField('location', types.StringType()),
        types.StructField('reputation', types.IntegerType()),
        types.StructField('up_votes', types.IntegerType()),
        types.StructField('down_votes', types.IntegerType()),
        types.StructField('views', types.IntegerType())])

sbad=sparkSess.read.format("s3selectCSV").schema(bdschema).options(header="true").options(delimiter="|").options(quote='\"').load("s3://bigdata-4/users.csv").select("index","id","age","creation_date","last_access_date","location","reputation","up_votes","down_votes","views")

sbad.write.mode("append").parquet("s3://bigdata-4/users_testing/")
