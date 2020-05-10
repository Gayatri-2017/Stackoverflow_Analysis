from pyspark import SparkContext

from pyspark.sql import SparkSession, types


sparkSess = SparkSession.builder.appName('tags').getOrCreate()
sc = sparkSess.sparkContext
bdschema = types.StructType([
        types.StructField('index', types.IntegerType()),
        types.StructField('id', types.IntegerType()),
        types.StructField('tag_name', types.StringType()),
        types.StructField('count', types.IntegerType()),
        types.StructField('excerpt_post_id', types.FloatType()),
        types.StructField('wiki_post_id', types.FloatType())])

sbad=sparkSess.read.format("s3selectCSV").schema(bdschema).options(header="true").options(delimiter="|").options(quote='\"').load("s3://bigdata-4/tags.csv").select("index","id","tag_name","count","excerpt_post_id","wiki_post_id")

sbad.write.mode("append").parquet("s3://bigdata-4/tags_new/")
