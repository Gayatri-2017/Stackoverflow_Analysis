DROP DATABASE IF EXISTS stackoverflow;
CREATE DATABASE stackoverflow;


DROP TABLE IF EXISTS stackoverflow.users;
CREATE EXTERNAL TABLE stackoverflow.users
(index int,
id int,
age int,
creation_date string,
last_access_date string,
location string,
reputation int,
up_votes int,
down_votes int,
views int)
STORED AS PARQUET LOCATION 's3a://bigdata-4/users/';

DROP TABLE IF EXISTS stackoverflow.post_history;
CREATE EXTERNAL TABLE stackoverflow.post_history
(index int,
id int,
creation_date string,
post_id int,
post_history_type_id int,
user_id int
)
STORED AS PARQUET LOCATION 's3a://bigdata-4/post_history/';

DROP TABLE IF EXISTS stackoverflow.posts_answers;
CREATE EXTERNAL TABLE stackoverflow.posts_answers
(
index int,
id int,title int,
accepted_answer_id float,
answer_count int,
comment_count int,
community_owned_date string,
creation_date string,
favorite_count int,
last_activity_date string,
last_edit_date string,
last_editor_user_id float,
owner_user_id int,
parent_id int,
post_type_id int,
score int,
tags int,
view_count int)
STORED AS PARQUET LOCATION 's3a://bigdata-4/post_answers/';


DROP TABLE IF EXISTS stackoverflow.tags;
CREATE EXTERNAL TABLE stackoverflow.tags
(
index int,
id int,
tag_name string,
count int,
excerpt_post_id float,
wiki_post_id float
)
STORED AS PARQUET LOCATION 's3a://bigdata-4/tags/';

DROP TABLE IF EXISTS stackoverflow.posts_questions;
CREATE EXTERNAL TABLE stackoverflow.posts_questions
(
index int,
id int,
title string,
accepted_answer_id float,
answer_count int,
comment_count int,
community_owned_date string,
creation_date string,
favorite_count int,
last_activity_date string,
last_edit_date string,
last_editor_user_id float,
owner_user_id int,
parent_id int,
post_type_id int,
score int,
tags string, 
view_count int)
STORED AS PARQUET LOCATION 's3://bigdata-4/posts_questions/';


DROP TABLE IF EXISTS stackoverflow.badges;
CREATE EXTERNAL TABLE stackoverflow.badges
(index int,
id int, 
name string,
user_id int,
class int)
STORED AS PARQUET LOCATION 's3a://bigdata-4/badges/';


