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
	artist VARCHAR,
	auth VARCHAR,
	firstName VARCHAR,
	gender VARCHAR,
	itemInSession VARCHAR,
	lastName VARCHAR,
	length VARCHAR,
	level VARCHAR,
	location VARCHAR,
	method VARCHAR,
	page VARCHAR,
	registration VARCHAR,
	sessionId INTEGER,
	song VARCHAR,
	status INTEGER,
	ts BIGINT,
	userAgent VARCHAR,
	userId integer SORTKEY
)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
	num_songs INTEGER,
	artist_id VARCHAR,
	artist_latitude VARCHAR,
	artist_longitude VARCHAR,
	artist_location VARCHAR,
	artist_name VARCHAR,
	song_id VARCHAR SORTKEY,
	title VARCHAR,
	duration DECIMAL(9),
	year INTEGER
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
	songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY SORTKEY,
	start_time TIMESTAMP NOT NULL,
	user_id INTEGER NOT NULL DISTKEY,
	level VARCHAR(10) NOT NULL,
	song_id VARCHAR(50) NOT NULL,
	artist_id VARCHAR(50) NOT NULL,
	session_id INTEGER NOT NULL,
	location VARCHAR(100) ,
	user_agent VARCHAR(250) 
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
	user_id INTEGER NOT NULL PRIMARY KEY SORTKEY,
	first_name VARCHAR(50) NOT NULL,
	last_name VARCHAR(50) NOT NULL,
	gender VARCHAR(1),
	level VARCHAR(10)
	) DISTSTYLE ALL
;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
	song_id VARCHAR(50) NOT NULL PRIMARY KEY SORTKEY,
	title VARCHAR(200) NOT NULL,
	artist_id VARCHAR(50) NOT NULL ,
	year INTEGER NOT NULL,
	duration DECIMAL(9) NOT NULL
) ;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
	artist_id VARCHAR(50) NOT NULL PRIMARY KEY SORTKEY,
	name VARCHAR(500) NOT NULL,
	location VARCHAR(250) ,
	latitude DECIMAL(9) ,
	longitude DECIMAL(9) 
	) DISTSTYLE ALL
;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
	start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
	hour INTEGER ,
	day INTEGER ,
	week INTEGER ,
	month INTEGER ,
	year INTEGER ,
	weekday INTEGER 
	) DISTSTYLE ALL
;
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
INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
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
	ON (se.artist = ss.artist_name
	AND se.song = ss.title
	AND ABS(se.length - ss.duration) <= 1 )
WHERE se.page = 'NextSong'
	AND se.artist IS NOT NULL
	AND se.song IS NOT NULL
	AND se.length IS NOT NULL
;
""")

user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level)
SELECT DISTINCT 
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
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
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT
    TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    DATE_PART(dayofweek, start_time) AS weekday
FROM staging_events se
WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
