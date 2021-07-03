import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

s3_config = config["S3"]
log_data_s3_path = s3_config.get("LOG_DATA")
song_data_s3_path = s3_config.get("SONG_DATA")
log_json_s3_path = s3_config.get("LOG_JSONPATH")

iam_config = config["IAM_ROLE"]
arn = iam_config.get("ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        event_id BIGINT IDENTITY PRIMARY KEY NOT NULL,
        artist VARCHAR(255),
        auth VARCHAR(255),
        first_name VARCHAR(255),
        gender VARCHAR (1),
        item_in_session INT,
        last_name VARCHAR(255),
        length DOUBLE PRECISION,
        level VARCHAR(255),
        location VARCHAR(255),
        method VARCHAR(30),
        page VARCHAR(30),
        registration VARCHAR(255),
        session_id BIGINT,
        song VARCHAR(255),
        status INT,
        ts VARCHAR(255),
        user_agent VARCHAR(255),
        user_id VARCHAR(255)
    )

""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        song_id VARCHAR(255) PRIMARY KEY,
        num_songs INT,
        title VARCHAR(255),
        artist_name VARCHAR(255),
        artist_latitude DOUBLE PRECISION,
        year INT,
        duration DOUBLE PRECISION,
        artist_id VARCHAR(255),
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR(255)
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay(
        songplay_id BIGINT IDENTITY PRIMARY KEY NOT NULL,
        start_time TIMESTAMP,
        user_id VARCHAR(255),
        level VARCHAR(255),
        song_id VARCHAR(255),
        artist_id VARCHAR(255),
        session_id BIGINT,
        location VARCHAR(255),
        user_agent VARCHAR(255)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id VARCHAR(255) PRIMARY KEY NOT NULL,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        gender VARCHAR (1),
        level VARCHAR(255)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR(255) PRIMARY KEY,
        title VARCHAR(255),
        artist_id VARCHAR(255),
        year INT,
        duration DOUBLE PRECISION
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR(255),
        name VARCHAR(255),
        artist_location VARCHAR(255),
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    )
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY
        staging_events 
    FROM
        '{log_data_s3_path}'
    iam_role '{arn}'
    json '{log_json_s3_path}'
""")

staging_songs_copy = (f"""
    COPY
        staging_songs
    FROM
        '{song_data_s3_path}'
    iam_role '{arn}'
    json 'auto'
""")

# FINAL TABLES

#https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
songplay_table_insert = ("""
    INSERT INTO songplay (
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        )
    SELECT DISTINCT TIMESTAMP 'epoch' + events.ts / 1000 * INTERVAL '1 second',
        events.user_id,
        events.level,
        songs.song_id,
        songs.artist_id,
        events.session_id,
        events.location,
        events.user_agent
    FROM staging_events AS events
        JOIN staging_songs AS songs ON events.artist = songs.artist_name
        AND events.song = songs.title
    WHERE events.page = 'NextSong'

""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events AS events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs AS songs
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, artist_location, artist_latitude, artist_longitude  )
    SELECT DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs AS songs
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + events.ts / 1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(HOUR FROM start_time) AS hour,
        EXTRACT(DAY FROM start_time) AS day,
        EXTRACT(WEEK FROM start_time) AS week,
        EXTRACT(MONTH FROM start_time) AS month,
        EXTRACT(YEAR FROM start_time) AS year,
        EXTRACT(WEEKDAY FROM start_time) AS weekday
    FROM staging_events AS events
    WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
