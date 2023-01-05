import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

HOST = config['CLUSTER']['HOST']
DB_NAME = config['CLUSTER']['DB_NAME']
DB_USER = config['CLUSTER']['DB_USER']
DB_PASSWORD = config['CLUSTER']['DB_PASSWORD']
DB_PORT = config['CLUSTER']['DB_PORT']

ARN = config['IAM_ROLE']['ARN']

LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
event_id BIGINT IDENTITY(0,1),
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
itemInSession varchar,
lastName varchar,
length varchar,
level varchar,
location varchar,
method varchar,
page varchar,
registration varchar,
sessionId integer,
song varchar,
status integer,
ts bigint,
userAgent varchar,
userId integer SORTKEY
)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
num_songs integer,
artist_id varchar,
artist_latitude varchar,
artist_longitude varchar,
artist_location varchar,
artist_name varchar,
song_id varchar SORTKEY,
title varchar,
duration decimal(9),
year integer
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (
songplay_id BIGINT IDENTITY(0,1) SORTKEY,
start_time timestamp NOT NULL,
user_id integer NOT NULL DISTKEY,
level varchar(10) NOT NULL,
song_id varchar(50) NOT NULL,
artist_id varchar(50) NOT NULL,
session_id integer NOT NULL,
location varchar(100) NULL,
user_agent varchar(250) NULL
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
user_id integer NOT NULL SORTKEY,
first_name varchar(50) NOT NULL,
last_name varchar(50) NOT NULL,
gender varchar(1),
level varchar(10)
) diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
song_id varchar(50) NOT NULL SORTKEY,
title varchar(200) NOT NULL,
artist_id varchar(50) NOT NULL ,
year integer NOT NULL,
duration decimal(9) NOT NULL
) ;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
artist_id varchar(50) NOT NULL SORTKEY,
name varchar(500) NOT NULL,
location varchar(250) NULL,
latitude decimal(9) NULL,
longitude decimal(9) NULL
) diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP NOT NULL SORTKEY,
hour integer NULL,
day integer NULL,
week integer NULL,
month integer NULL,
year integer NULL,
weekday integer NULL
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json  {}
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' as start_time,
    se.userId,
    se.level,
    ss.song_id song_id,
    ss.artist_id artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_events se
JOIN staging_songs ss
ON (se.artist = ss.artist_name)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level)
SELECT DISTINCT 
    userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
FROM staging_events se
WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration)
SELECT DISTINCT 
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,latitude,longitude)
SELECT DISTINCT
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT
    TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' as start_time,
    EXTRACT(hour FROM start_time) as hour,
    EXTRACT(day FROM start_time) as day,
    EXTRACT(week FROM start_time) as week,
    EXTRACT(month FROM start_time) as month,
    EXTRACT(year FROM start_time) as year,
    DATE_PART(dayofweek, start_time) as weekday
FROM staging_events se
WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
