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
    return [int(arr[0]) if arr[0] else None,int(arr[1]) if arr[1] else None,arr[2],int(arr[3]) if arr[3] else None,int(arr[4]) if arr[4] else None,int(arr[5]) if arr[5] else None]

def savetheresult( rdd ):
    if not rdd.isEmpty():
        nrdd = rdd.map(call_split)
        df = sparkSess.createDataFrame(nrdd,schema)
        #df.show()
        #df.coalesce(2).write.mode("append").parquet("s3://bigdata-4/parquet/")
        df.coalesce(2).write.mode("append").parquet("s3://bigdata-4/post_history/")
        
sparkSess = SparkSession.builder.appName('post_history_table').getOrCreate()
sc = sparkSess.sparkContext
schema = types.StructType([
        types.StructField('unnamed', types.IntegerType()),
        types.StructField('id', types.IntegerType()),
        types.StructField('creation_date', types.StringType()),
        types.StructField('post_id', types.IntegerType()),
        types.StructField('post_history_type_id', types.IntegerType()),
        types.StructField('user_id', types.IntegerType())])
ssc = StreamingContext(sc, 1)
kvs = KafkaUtils.createDirectStream(ssc, ["post_history"], {"bootstrap.servers": "127.0.0.1:9092"})
lines = kvs.map(lambda x: x[1])
#lines.pprint()
lines2=lines.foreachRDD(savetheresult)
ssc.start()
ssc.awaitTermination()



