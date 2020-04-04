import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS app_user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stage_event
    (
        artist          TEXT,
        auth            TEXT,
        first_name      TEXT,
        gender          TEXT,
        item_in_session INTEGER,
        last_name       TEXT,
        length          FLOAT4,
        level           TEXT,
        location        TEXT,
        method          TEXT,
        page            TEXT,
        registration    FLOAT8,
        session_id      INTEGER,
        song            TEXT,
        status          INTEGER,
        ts              BIGINT,
        user_agent      TEXT,
        user_id         TEXT
    );
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
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay
    (
        sp_songplay_id      BIGINT IDENTITY(1, 1) PRIMARY KEY,
        sp_start_time       TIMESTAMP NOT NULL SORTKEY,
        sp_song_id          TEXT,
        sp_artist_id        TEXT,
        sp_user_id          TEXT NOT NULL DISTKEY,
        sp_level            TEXT,
        sp_session_id       INTEGER,
        sp_item_in_session  INTEGER,
        sp_location         TEXT,
        sp_user_agent       TEXT
    ) diststyle key;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS app_user
    (
        u_user_id       TEXT PRIMARY KEY SORTKEY,
        u_first_name    TEXT,
        u_last_name     TEXT,
        u_gender        TEXT,
        u_level         TEXT
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song
    (
        s_song_id       TEXT PRIMARY KEY SORTKEY,
        s_title         TEXT,
        s_artist_id     TEXT DISTKEY,
        s_year          SMALLINT,
        s_duration      FLOAT4
    ) diststyle key;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist
    (
        a_artist_id     TEXT PRIMARY KEY SORTKEY,
        a_name          TEXT,
        a_location      TEXT,
        a_latitude      FLOAT4,
        a_longitude     FLOAT4
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        t_start_time    TIMESTAMP PRIMARY KEY SORTKEY,
        t_hour          SMALLINT,
        t_day           SMALLINT,
        t_week          SMALLINT,
        t_month         SMALLINT,
        t_year          SMALLINT DISTKEY,
        t_weekday       SMALLINT
    ) diststyle key;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {} region '{}';
""").format(
    'stage_event',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
    config['CLUSTER']['REGION']
)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto' region '{}';
""").format(
    'stage_song',
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['CLUSTER']['REGION']
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (sp_start_time, sp_song_id, sp_artist_id, sp_user_id, sp_level, sp_session_id, sp_item_in_session, sp_location, sp_user_agent) SELECT
        TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
        s.song_id,
        s.artist_id,
        e.user_id,
        e.level,
        e.session_id,
        e.item_in_session,
        e.location,
        e.user_agent
    FROM stage_event e, stage_song s
    WHERE
        e.song = s.title AND
        e.artist = s.artist_name AND
        ABS(e.length - s.duration) < 2
""")

user_table_insert = ("""
    INSERT INTO app_user SELECT DISTINCT (user_id)
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM stage_event
""")

song_table_insert = ("""
    INSERT INTO song SELECT DISTINCT (song_id)
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM stage_song
""")

artist_table_insert = ("""
    INSERT INTO artist SELECT DISTINCT (artist_id)
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM stage_song
""")


time_table_insert = ("""
    INSERT INTO time
        WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM stage_event)
        SELECT DISTINCT
        ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
