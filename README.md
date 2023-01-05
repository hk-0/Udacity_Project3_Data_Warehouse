# Project Sparkify - Data Warehouse

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.


## Project Description

In this project, I will apply what I've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, I will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Database Model and Schema

The data model is implemented using a star schema. The schema contains two staging tables,`staging_events` and `staging_songs`, one fact table, `songplays`, and four dimension tables: `songs`, `artists`, `users` and `time`.

#### Staging Tables

**`staging_events`**
| COLUMN         | TYPE                 |
|----------------|----------------------|
| event_id       | BIGINT IDENTITY(1,1) |
| artist         | varchar              |
| auth           | varchar              |
| firstName      | varchar              |
| gender         | varchar              |
| itemInSession  | varchar              |
| lastName       | varchar              |
| length         | varchar              |
| level          | varchar              |
| location       | varchar              |
| method         | varchar              |
| page           | varchar              |
| registration   | varchar              |
| sessionId      | integer              |
| song           | varchar              |
| status         | integer              |
| ts             | bigint               |
| userAgent      | varchar              |
| userId         | integer              |

**`staging_songs`**
| COLUMN           | TYPE       |
|------------------|------------|
| num_songs        | integer    |
| artist_id        | varchar    |
| artist_latitude  | varchar    |
| artist_longitude | varchar    |
| artist_location  | varchar    |
| artist_name      | varchar    |
| song_id          | varchar    |
| title            | varchar    |
| duration         | decimal(9) |
| year             | integer    |


#### Fact Tables

**`songplay`**
| COLUMN      | TYPE      | CONSTRAINT  |
|-------------|-----------|-------------|
| songplay_id | SERIAL    | PRIMARY KEY |
| start_time  | TIMESTAMP | NOT NULL    |
| user_id     | INT       | NOT NULL    |
| level       | VARCHAR   |             |
| song_id     | VARCHAR   |             |
| artist_id   | VARCHAR   |             |
| session_id  | INT       |             |
| location    | VARCHAR   |             |
| user_agent  | VARCHAR   |             |

#### Dimension Tables

**`users`**
| COLUMN     | TYPE    | CONSTRAINT  |
|------------|---------|-------------|
| user_id    | INT     | PRIMARY KEY |
| first_name | VARCHAR | NOT NULL    |
| last_name  | VARCHAR | NOT NULL    |
| gender     | VARCHAR |             |
| level      | VARCHAR |             |

**`songs`**
| COLUMN    | TYPE    | CONSTRAINT  |
|-----------|---------|-------------|
| song_id   | VARCHAR | PRIMARY KEY |
| title     | VARCHAR | NOT NULL    |
| artist_id | VARCHAR | NOT NULL    |
| year      | INT     | NOT NULL    |
| duration  | NUMERIC | NOT NULL    |

**`artist`**
| COLUMN    | TYPE             | CONSTRAINT  |
|-----------|------------------|-------------|
| artist_id | VARCHAR          | PRIMARY KEY |
| name      | VARCHAR          | NOT NULL    |
| location  | VARCHAR          |             |
| latitude  | DOUBLE PRECISION |             |
| longitude | DOUBLE PRECISION |             |

**`time`**
| COLUMN     | TYPE      | CONSTRAINT  |
|------------|-----------|-------------|
| start_time | TIMESTAMP | PRIMARY KEY |
| hour       | INT       |             |
| day        | INT       |             |
| week       | INT       |             |
| month      | INT       |             |
| year       | INT       |             |
| weekday    | INT       |             |

## File Structure And Description

```.
├── create_tables.py (create_tables.py contains sql scripts to create the required database tables with the appropriate constraints)
├── dwh.cfg (config file that contains the AWS keys, redshift cluster configuration and S3 bucket with the dataset)
├── etl.py (etl.py loads data from S3 into staging tables on Redshift and then process that data into the analytics tables on Redshift )
├── README.md
├── sql_queries.py (Contains all the create sql queries needed to create the database tables when calling create_tables.py. Also containts the S2 copy commands to load the data into the staging tables and Insert sqls to insert data returned by etl.py)
└── etl_aws_setup_and_test.ipynb (Jupyter Notebook to create the IAM role, get ARN, create redshift cluster on AWS and query and validate the data loaded )
```
## Project Dataset

S3 data path

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`
Log data json path: `s3://udacity-dend/log_json_path.json`

### Song Dataset

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

`song_data/A/B/C/TRABCEI128F424C983.json`
`song_data/A/A/B/TRAABJL12903CDCF1A.json`

And below is an example of what a single song file, `TRAABJL12903CDCF1A.json`, looks like.

> {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### Log Dataset

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.
The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

`log_data/2018/11/2018-11-12-events.json`
`log_data/2018/11/2018-11-13-events.json`

And below is an example of what the data in a log file, `2018-11-12-events.json`, looks like.

![log_file_example](/log_file_ex.png)

## Execution

I have created the etl_aws_setup_and_test.ipynb Jupyter notebook as a helpful guide to performing all the steps needed from creating the necessary IAM role to starting a redshift cluster and loading data from S3 bucket into the appropriate staging tables and transforming the data and then validating the loaded data.


## Example Queries

### Example SQL 1 : Count of users by subscription level that listened to music on sparkify in 2018.

``` select u.level,t.year as year,count(distinct u.user_id) 
from songplay s join time t on s.start_time = t.start_time 
join users u on s.user_id = u.user_id
where t.year = 2018  
group by u.level,t.year;
```

| level | year | count |
|------:|-----:|------:|
|  paid | 2018 |    22 |
|  free | 2018 |    82 |

### Example SQL 2 : Count of users online by time of day. Break the day into 4 parts - morning (6 am to 12 pm), afternoon(12 pm to 6 pm), evening (6 pm to 12 am) and night (12 am to 6 am)

``` select case 
        when t.hour >=0 and t.hour <6 then 'Night'
        when t.hour >=6 and t.hour <12 then 'Morning' 
        when t.hour >=12 and t.hour <18 then 'Afternoon' 
        else 'Evening' END as time_of_day
    ,count(distinct user_id) from songplay s join time t on s.start_time = t.start_time group by 1;
```
    
| time_of_day | count |
|------------:|------:|
|   Afternoon |    78 |
|       Night |    57 |
|     Morning |    58 |
|     Evening |    58 |

### Example SQL 3: Top 5 Most Listened Artist among Female users

``` select a.name,u.gender, count(songplay_id)
from songplay s join users u on s.user_id = u.user_id
join artists a on s.artist_id = a.artist_id
where u.gender ='F'
group by a.name,u.gender
order by  count(songplay_id) desc limit 5;
```

|            name | gender | count |
|----------------:|-------:|------:|
|            Muse |      F |  1008 |
|   Kings Of Leon |      F |   864 |
|       Radiohead |      F |   768 |
|        Coldplay |      F |   744 |
| Alliance Ethnik |      F |   630 |

### Example SQL 4: Top 5 Most Listened Artist among Male users

``` select a.name,u.gender as user_gender, count(songplay_id)
from songplay s join users u on s.user_id = u.user_id
join artists a on s.artist_id = a.artist_id
where u.gender ='M'
group by a.name,u.gender
order by  count(songplay_id) desc limit 5;
```

|         name | user_gender | count |
|-------------:|------------:|------:|
|    Radiohead |           M |   408 |
| Foo Fighters |           M |   234 |
|     Coldplay |           M |   192 |
|         Muse |           M |   189 |
|   Jason Mraz |           M |   156 |