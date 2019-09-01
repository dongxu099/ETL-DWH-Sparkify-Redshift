import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

## Create Staging tables
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist        VARCHAR,
        auth          VARCHAR,
        firstName     VARCHAR,
        gender        VARCHAR,
        itemInSession VARCHAR,
        lastName      VARCHAR,
        length        DOUBLE PRECISION,
        level         VARCHAR,
        location      VARCHAR,
        method        VARCHAR,
        page          VARCHAR,
        registration  VARCHAR,
        sessionId     INTEGER,
        song          VARCHAR,
        status        INTEGER,   -- HTTP STATUS	
        ts            BIGINT,
        userAgent     VARCHAR,
        userId        INTEGER)
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     VARCHAR,
        artist_longitude    VARCHAR,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            DOUBLE PRECISION,
        year                INTEGER)
""")

## Create Analytics tables
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id   INT IDENTITY(0,1),
        start_time    TIMESTAMP,
        user_id       VARCHAR,
        level         VARCHAR,
        artist_id     VARCHAR,
        song_id       VARCHAR,
        session_id    VARCHAR,
        location      VARCHAR,
        user_agent    VARCHAR)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id       INTEGER,
        first_name    VARCHAR,
        last_name     VARCHAR, 
        gender        VARCHAR,
        level         VARCHAR) diststyle all
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id       VARCHAR,
        title         VARCHAR,
        artist_id     VARCHAR,
        year          INTEGER,
        duration      DOUBLE PRECISION)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id     VARCHAR,
        name          VARCHAR, 
        location      VARCHAR, 
        latitude      VARCHAR, 
        longitude     VARCHAR) diststyle all
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time    TIMESTAMP,
        hour          SMALLINT,
        day           SMALLINT,
        week          SMALLINT,
        month         SMALLINT,
        year          SMALLINT,
        weekday       SMALLINT) diststyle all
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM {} 
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON {}
    region 'us-west-2'
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM {} 
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
    region 'us-west-2'
""").format(config.get('S3','SONG_DATA'), 
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time,
        user_id,
        level,
        artist_id,
        song_id,
        session_id,
        location,
        user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
               * INTERVAL '1 second'   AS start_time,
           se.userId                   AS user_id,
           se.level                    AS level,
           ss.artist_id                AS artist_id,
           ss.song_id                  AS song_id,
           se.sessionId                AS session_id,
           se.location                 AS location,
           se.userAgent                AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss
        ON se.artist = ss.artist_name AND 
           se.song = ss.title AND
           se.length = ss.duration
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level)
    SELECT DISTINCT se.userId          AS user_id,
           se.firstName                AS first_name,
           se.lastName                 AS last_name,
           se.gender                   AS gender,
           se.level                    AS level
    FROM staging_events AS se
    WHERE se.page = 'NextSong' AND se.userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id, 
        title, 
        artist_id, 
        year, 
        duration)
    SELECT DISTINCT ss.song_id         AS song_id,
           ss.title                    AS title,
           ss.artist_id                AS artist_id,
           ss.year                     AS year,
           ss.duration                 AS duration
    FROM staging_songs AS ss
    WHERE ss.song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude)
    SELECT DISTINCT ss.artist_id       AS artist_id,
           ss.artist_name              AS name,
           ss.artist_location          AS location,
           ss.artist_latitude          AS latitude,
           ss.artist_longitude         AS longitude
    FROM staging_songs AS ss
    WHERE ss.artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
               * INTERVAL '1 second'        AS start_time,
           EXTRACT(hour FROM start_time)    AS hour,
           EXTRACT(day FROM start_time)     AS day,
           EXTRACT(week FROM start_time)    AS week,
           EXTRACT(month FROM start_time)   AS month,
           EXTRACT(year FROM start_time)    AS year,
           EXTRACT(week FROM start_time)    AS weekday
    FROM staging_events AS se
    WHERE se.page = 'NextSong' AND se.ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create,
                        songplay_table_create]

drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert, 
                        user_table_insert, 
                        song_table_insert, 
                        artist_table_insert, 
                        time_table_insert]
