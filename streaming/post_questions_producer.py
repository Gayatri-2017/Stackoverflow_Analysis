import boto3
import pandas as pd
import time
import json
import custom_kafka_producer

client = boto3.client('s3') #access key id, secret access and secret token is provided in .aws/credentials file.
resource = boto3.resource('s3')
my_bucket = resource.Bucket('bigdata-4')
obj = client.get_object(Bucket='bigdata-4', Key='posts_questions.csv')
_producer = None
kafka_producer = custom_kafka_producer.connect_kafka_producer()
for line in pd.read_csv(obj['Body'],sep='|',chunksize =1):
    time.sleep(1)
    result = line.to_csv(sep = '|',header=False,index=False)
    print(result)
    custom_kafka_producer.publish_message(kafka_producer,'post_questions', 'posts_questions.csv',result)

