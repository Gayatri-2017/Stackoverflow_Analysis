#!/bin/bash
################################ for upgrading Python to 3.6.8################################################################################

sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh


####################################### for setting up the service account details for GCP ###################################################

aws s3 cp s3://bigdata-4/atomic-rune-258901-b1e6b7a7dcfa.json .
sudo pip install --upgrade google-cloud-bigquery
export GOOGLE_APPLICATION_CREDENTIALS="home/hadoop/atomic-rune-258901-b1e6b7a7dcfa.json"


##################################### for installing other packages ##########################################
sudo pip install boto3
sudo pip install pandas
sudo pip install kafka-python


###################################### for setting up the Kakfa in EMR Cluster ###############################################################

wget http://www-us.apache.org/dist/kafka/2.2.1/kafka_2.12-2.2.1.tgz
tar xzf kafka_2.12-2.2.1.tgz
sudo mv kafka_2.12-2.2.1 /usr/local/kafka
cd /usr/local/kafka/
sudo mkdir /usr/local/kafka/custom_logs
sudo chmod 777 custom_logs
nohup ./bin/kafka-server-start.sh config/server.properties >> custom_logs/kafka.log 2>&1 &

