### Installing

#### Setting up AWS EMR cluster

Create an Amazon AWS account. Create a S3 Bucket where the required data will be stored. 
Set up AWS CLI in your local system and edit the credentials in ```~/.aws/credentials``` file.


Place the ```startup.sh``` file in S3 bucket. The ```startup.sh``` would install the ```kafka-python``` library, ```Boto3```,  upgrade the python version for ```pyspark```, install 

Replace [cluster_name] with your cluster name, [your_bucket_name] with your bucket name and [path_to_startup.sh] with the startup.sh path.

```
aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --applications Name=Hadoop Name=Hive Name=Pig Name=Hue Name=Spark Name=HBase Name=ZooKeeper --ebs-root-volume-size 10 --ec2-attributes '{"KeyName":"bigdata","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-11f4922f","EmrManagedSlaveSecurityGroup":"sg-0cdd7be7c0461fca4","EmrManagedMasterSecurityGroup":"sg-044309ee81aa1d976"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.28.0 --log-uri 's3n://bigdata-4/' --name '[cluster_name]' --instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m4.xlarge","Name":"Core - 2"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m4.xlarge","Name":"Master - 1"}]' --configurations '[{"Classification":"hbase","Properties":{"hbase.emr.storageMode":"s3"}},{"Classification":"hbase-site","Properties":{"hbase.rootdir":"s3://bigdata-4"}}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1 --bootstrap-actions Path="s3://[your_bucket_name]/[path_to_startup.sh]/startup1.sh"
```
In the python files used, the path of s3 bucket is set to ```s3://bigdata-4/```

While creating the AWS cluster, replace the security group for the account you are logged in.

#### Creating Hive Tables

Run the command below to create the required tables on HIVE

```beeline -u 'jdbc:hive2://localhost:10000/default -n hadoop@ec2-18-233-63-126.compute-1.amazonaws.com' -f tables_creation.hql```

the above command would create ```users.hql```, ```post_history.hql```, ```posts_answers.hql```, ```posts_questions.hql```, ```tags.hql``` and ```badges.hql``` tables


### Running

Change ```s3://bigdata-4/``` to ```s3://[your_bucket_name]``` in the below python files

To create direct data streams using Kafka, run the file ```[table_name]_producer.py```

Using another EMR instance, run the ```[table_name]_stream.py``` 
You can see the Parquet files being created in the bucket ```s3://[your_bucket_name]/[table_name]/```

```[table_name]``` is the name of the Hive table created in previous step which should be one of ```users```,   ```post_history```, ```posts_answers```, ```posts_questions```, ```tags``` or ```badges```



