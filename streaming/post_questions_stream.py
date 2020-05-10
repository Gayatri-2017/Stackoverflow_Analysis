from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession, types

#def saveRecord(rdd):
    #host = 'nml-cloud-152.cs.sfu.ca'
    #table = 'test'
    #keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    #valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    #conf = {"hbase.zookeeper.quorum": host,
       #"hbase.mapred.outputtable": table,
        #"mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        #"mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        #"mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    #datamap = rdd.map(lambda x: (str(json.loads(x)["row_key"]),[str(json.loads(x)["row_key"]),"test_cf","test1",x]))
    #datamap.saveAsHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)
def call_split(line):
    arr=line.split('|')
    #print("size: ",len(arr))
    return [int(float(arr[0].replace('"', ''))) if arr[0] else None,int(float(arr[1].replace('"', ''))) if arr[1] else None,arr[2],float(arr[3].replace('"', '')) if arr[3] else None,int(float(arr[4].replace('"', ''))) if arr[4] else None,int(float(arr[5].replace('"', ''))) if arr[5] else None,arr[6], arr[7],int(float(arr[8].replace('"', ''))) if arr[8] else None, arr[9], arr[10],float(arr[11].replace('"', '')) if arr[11] else None,int(float(arr[12].replace('"', ''))) if arr[12] else None,arr[13],int(float(arr[14].replace('"', ''))) if arr[14] else None,int(float(arr[15].replace('"', ''))) if arr[15] else None,arr[16],int(float(arr[17].replace('"', ''))) if arr[17] else None]


def savetheresult( rdd ):
    if not rdd.isEmpty():
        nrdd = rdd.map(call_split)
        df = sparkSess.createDataFrame(nrdd,schema)
        #df.show()
        #df.coalesce(2).write.mode("append").parquet("s3://bigdata-4/parquet/")
        df.coalesce(2).write.mode("append").parquet("s3://bigdata-4/post_questions/")
        
sparkSess = SparkSession.builder.appName('post_table').getOrCreate()
sc = sparkSess.sparkContext
schema = types.StructType([
        types.StructField('unnamed', types.IntegerType()),
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
        
ssc = StreamingContext(sc, 1)
kvs = KafkaUtils.createDirectStream(ssc, ["post_questions"], {"bootstrap.servers": "127.0.0.1:9092"})
lines = kvs.map(lambda x: x[1])
#lines.pprint()
lines2=lines.foreachRDD(savetheresult)
ssc.start()
ssc.awaitTermination()



