import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stage_event
    (
        artist          TEXT,
        auth            TEXT,
        firstName       TEXT,
        gender          TEXT,
        itemInSession   INTEGER,
        lastName        TEXT,
        length          FLOAT4,
        level           INTEGER,
        location        TEXT,
        method          TEXT,
        page            TEXT,
        registration    FLOAT8,
        sessionId       INTEGER,
        song            TEXT,
        status          INTEGER,
        ts              BIGINT,
        userAgent       TEXT,
        userId          TEXT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stage_song
    (
        song_id             TEXT,
        title               TEXT,
        duration            FLOAT4,
        year                SMALLINT,
        artist_id           TEXT,
        artist_name         TEXT,
        artist_latitude     REAL,
        artist_longitude    REAL,
        artist_location     TEXT,
        num_songs           INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay
    (

    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user
    (

    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song
    (

    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist
    (

    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (

    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE {}
    JSON {};
""").format(
    'stage_event',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH']
)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE {}
    JSON {};
""").format(
    'stage_song',
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    'AUTO'
)

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
