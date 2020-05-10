from pyspark import SparkContext

from pyspark.sql import SparkSession, types


sparkSess = SparkSession.builder.appName('posts_questions').getOrCreate()
sc = sparkSess.sparkContext
bdschema = types.StructType([
        types.StructField('index', types.IntegerType()),
        types.StructField('id', types.IntegerType()),
        types.StructField('title', types.StringType()),
        types.StructField('accepted_answer_id', types.FloatType()),
        types.StructField('answer_count', types.IntegerType()),
        types.StructField('comment_count', types.IntegerType()),
        types.StructField('community_owned_date', types.StringType()),
        types.StructField('creation_date', types.StringType()),
        types.StructField('favorite_count', types.IntegerType()),
        types.StructField('last_activity_date', types.StringType()),
        types.StructField('last_edit_date', types.StringType()),
        types.StructField('last_editor_user_id', types.FloatType()),
        types.StructField('owner_user_id', types.IntegerType()),
        types.StructField('parent_id', types.StringType()),
        types.StructField('post_type_id', types.IntegerType()),
        types.StructField('score', types.IntegerType()),
        types.StructField('tags', types.StringType()),
        types.StructField('view_count', types.IntegerType())])
 
sbad=sparkSess.read.format("s3selectCSV").schema(bdschema).options(header="true").options(delimiter="|").options(quote='\"').load("s3://bigdata-4/posts_questions.csv").select("index","id","title","accepted_answer_id","answer_count","comment_count","community_owned_date","creation_date","favorite_count","last_activity_date","last_edit_date","last_editor_user_id","owner_user_id","parent_id","post_type_id","score","tags","view_count")

sbad.write.mode("append").parquet("s3://bigdata-4/posts_questions_new/")
