# Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Project Datasets

We will be working with two datasets that reside in S3. Here are the S3 links for each:

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

## Goal
- Build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 

# How to run ?

- First we need to AWS access key id and AWS secret key access for the respect IAM user enlisted in a ***dl.cfg***.
    This is later parsed by etl.py.
    
> AWS_ACCESS_KEY_ID= ''
> AWS_SECRET_ACCESS_KEY= ''

- We aslo need to specify a new S3 bucket with both read and write roles ( full access).
- Finally, we will run    etl.py   in the terminal.


# Project Structure

- **dl.cfg** : Contains AWS access key and secret access key.
- **etly.py**: Read data from public S3 bucket and using the power of Spark performed data wrangling and data transformation to create dimension and fact
            tables in parquet format which is further pushed to the S3 bucket created for the IAM user.
            
- **README.md**: For the detailed documentaion of the ETL development.
- **Testing.ipynb**: Tested codes before implementing inside the automated etl.py file.

# ETL Pipeline:
 1. Parse the dl.cfg to get access of the S3 bucket.
 
 2. Creating a spark session ;    create_spark_session()    .
 
 3. Then, process the song data by running    process_song_data()    .
     - This function will create two dimensions tables namely
         song and artist.
    
     - Finally, the dimension table will be written to the S3 bucket 
       in parquet format which saves space and makes reading more efficient.
 
 4.     process_log_data()    will process and transform the log files:
        - This block will create two dimensions tables namely users and time.
        
        - Also, we will create a songplay fact table by reading the song table resides in S3 in the parquet form and 
          the log table from public S3 bucket        s3a://udacity-dend/        , and by aplying a join condition on the
          song title name.
          
        - Finally, we will push all the dimension and fact tables in to the S3 bucket        s3a://jubin-sparkify-datalake/     
        
# Schema for Song Play Analysis

Using the song and log datasets, here I have created a star schema optimized for queries on song play analysis. This includes the following tables.

### Song Table
 - **Type**: Dimension Table
| Column     | Type    |
|------------|---------|
| song_id    | string  |
| song_title | string  |
| artist_id  | string  |
| year       | integer |
| duration   | double  |

### Artist Table

 - **Type**: Dimension Table
| Column          | Type   |
|-----------------|--------|
| artist_id       | string |
| artist_name     | string |
| artist_location | string |
| latitude        | double |
| longitude       | double |

### Users Table

- **Type**: Dimension Table

| Column    | Type    |
|-----------|---------|
| user_id   | integer |
| gender    | string  |
| level     | string  |
| firstName | string  |
| lastName  | string  |

### Time Table

- **Type**: Dimension Table

| Column  | Type      |
|---------|-----------|
| time    | timestamp |
| hour    | integer   |
| day     | integer   |
| week    | integer   |
| weekday | string    |
| month   | integer   |
| year    | integer   |

### Songplay Table

- **Type**: Fact Table


| Column      | Type      |
|-------------|-----------|
| Songplay_Id | integer   |
| user_id     | integer   |
| song_id     | string    |
| artist_id   | string    |
| session_id  | integer   |
| level       | string    |
| location    | string    |
| userAgent   | string    |
| start_time  | timestamp |
 
        

        




