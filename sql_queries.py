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
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        event_id BIGINT IDENTITY PRIMARY KEY NOT NULL,
        artist VARCHAR(255),
        auth VARCHAR(255),
        firstName VARCHAR(255),
        gender VARCHAR (1),
        itemInSession INT,
        lastName VARCHAR(255),
        length DOUBLE PRECISION,
        level VARCHAR(255),
        location VARCHAR(255),
        method VARCHAR(30),
        page VARCHAR(30),
        registration VARCHAR(255),
        sessionId BIGINT,
        song VARCHAR(255),
        status INT,
        ts VARCHAR(255),
        userAgent VARCHAR(255),
        userId VARCHAR(255)
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
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY
        staging_events 
    FROM
        {log_data_s3_path}
    iam_role {arn}
    json {log_json_s3_path}
""")

staging_songs_copy = (f"""
    COPY
        staging_songs
    FROM
        {song_data_s3_path}
    iam_role {arn}
    json 'auto'
""")
print(staging_songs_copy)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
