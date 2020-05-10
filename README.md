# Unstack the “Big Stack”

## Team Members:
1. Ankita Kundra
2. Gayatri Ganapathy
3. Kunal Niranjan Desai
4. Ria Gupta

## Data
https://bigquery.cloud.google.com/dataset/bigquery-public-data:stackoverflow

## Tableau link to our project:
https://public.tableau.com/views/StackOverflow_Analysis/UserAnalysis?:display_count=y&publish=yes&:origin=viz_share_link

## Steps to run the project:

1. Use the Google Big Query API to query the data from Bigquery Dataset.
2. Upload the files onto Amazon S3 bucket and set up the Amazon EMR.
3. Upload the files onto Amazon S3 bucket and set up the Amazon EMR.
4. Use startup.sh to set up the environment in AWS CLI.
5. Run table_name_producer.py and table_name_stream.py to run the Kafka producer and consumer process.
6. See if parquet files are formed in the cluster under the specified location.
7. Use Tableau link above to visualize the analysis made on the data.











