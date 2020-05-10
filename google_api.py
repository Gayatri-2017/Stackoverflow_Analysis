from google.cloud import bigquery
from io import StringIO # python3; python2: BytesIO 
import boto3
import pandas as pd
import numpy as np
import string

def remove_non_ascii(text):
    return ''.join(i for i in text if ord(i)<128)


client = bigquery.Client()
stack_dataset_ref = client.dataset('stackoverflow', project='bigquery-public-data')
stack_dset = client.get_dataset(stack_dataset_ref)
stack_badges = client.get_table(stack_dset.table('badges'))

sql = """
SELECT table_name
FROM `bigquery-public-data.stackoverflow.INFORMATION_SCHEMA.COLUMNS`
"""

df = client.query(sql).to_dataframe()
#tables = list(df.table_name.unique())
tables = ['posts_questions', 'posts_answers', 'users', 'tags', 'post_history']
for table in tables:
    print(table)
    if table == 'users':
        sql_table = """
            SELECT id, age, creation_date, last_access_date, location, reputation, up_votes, down_votes, views
            FROM  `bigquery-public-data.stackoverflow.users` WHERE id IS NOT NULL
            LIMIT 200000
            """

    elif table == 'posts_answers':
        sql_table = """
            SELECT id, title, accepted_answer_id, answer_count, comment_count, community_owned_date, creation_date, favorite_count, last_activity_date, last_edit_date, last_editor_user_id, owner_user_id, parent_id, post_type_id, score, tags, view_count
            FROM  `bigquery-public-data.stackoverflow.posts_answers` WHERE owner_user_id IS NOT NULL
            LIMIT 200000
            """

    elif table == 'posts_questions':
        sql_table = """
            SELECT id, title, accepted_answer_id, answer_count, comment_count, community_owned_date, creation_date, favorite_count, last_activity_date, last_edit_date, last_editor_user_id, owner_user_id, parent_id, post_type_id, score, tags, view_count
            FROM  `bigquery-public-data.stackoverflow.posts_questions` WHERE owner_user_id IS NOT NULL
            LIMIT 200000
            """
            
    elif table == 'post_history':
        sql_table = """
            SELECT id, creation_date, post_id, post_history_type_id, user_id
            FROM  `bigquery-public-data.stackoverflow.post_history` WHERE user_id IS NOT NULL
            LIMIT 200000
            """
        
    else:
        sql_table = """
                SELECT *
                FROM  `bigquery-public-data.stackoverflow.%s`
                LIMIT 200000
        """ % table

    table_df = client.query(sql_table).to_dataframe()
    # if table == 'users':
    #     # table_df = table_df.drop(columns = ['display_name'])
    #     #table_df[table_df['location'].translate(None, string.punctuation).isalnum()]
    #     #table_df['location'] = table_df['location'].str.encode('ascii', 'ignore').str.decode('ascii')
    #     table_df['location'] = table_df['location'].str.replace('"','')

    for column in table_df:
        if (table_df[column].apply(type)==str).any():
            table_df[column] = table_df[column].str.encode('ascii', 'ignore').str.decode('ascii')
            table_df[column] = table_df[column].str.replace('"','')
    
    bucket = 'bigdata-4' # already created on S3
    csv_buffer = StringIO()
    table_df.to_csv(csv_buffer, sep='|')
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, table+'.csv').put(Body=csv_buffer.getvalue())
    print(table+' uploaded to s3')

# for table in list(client.list_dataset_tables(stack_dset)):
#     print(table)
#rows = client.list_rows(stack_badges)
#for row in rows:
#    print(row)
# sql1 = """
#     SELECT *
#     FROM  `bigquery-public-data.stackoverflow.badges`
#     LIMIT 100000
# """

# sql2 = """
#     SELECT *
#     FROM  `bigquery-public-data.stackoverflow.comments`
#     LIMIT 100000
# """
# sql3 = """
#     SELECT *
#     FROM  `bigquery-public-data.stackoverflow.comments`
#     LIMIT 100000
# """



# # df = client.query(sql).to_dataframe()
# #df1 = client.query(sql1).to_dataframe()
# #print(df1)

# #df.to_csv('/home/kdesai/badges')
# #print(df1)
# badges = client.query(sql1).to_dataframe()
# # print(df2['last_activity_date'])
# df3 = df3[df3.last_activity_date<df3.last_activity_date.quantile(0.25)]
# print(df3)




