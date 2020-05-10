from pyspark import SparkContext

from pyspark.sql import SparkSession, types


sparkSess = SparkSession.builder.appName('badges').getOrCreate()
sc = sparkSess.sparkContext
bdschema = types.StructType([
        types.StructField('id', types.IntegerType()),
        types.StructField('name', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('user_id', types.IntegerType()),
        types.StructField('class', types.IntegerType()),
        types.StructField('tag_based', types.BooleanType())])

sbad=sparkSess.read.format("s3selectCSV").schema(bdschema).options(header="true").load("s3://bigdata-4/badges.csv").select("id","name")

sbad.write.mode("append").parquet("s3://bigdata-4/badges/")
