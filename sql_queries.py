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
    IAM_ROLE {}
    JSON {} region 'us-west-2';
""").format(
    'stage_event',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH']
)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE {}
    JSON {} region 'us-west-2';
""").format(
    'stage_song',
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    'AUTO'
)

# FINAL TABLES

songplay_table_insert = ("""
    SELECT
        e.ts                as sp_start_time,
        s.song_id           as sp_song_id,
        s.artist_id         as sp_artist_id,
        e.userId            as sp_user_id,
        e.level             as sp_level,
        e.session_id        as sp_session_id,
        e.item_in_session   as sp_item_in_session,
        e.location          as sp_location,
        e.user_agent        as sp_user_agent
    INTO songplay
    FROM stage_event e, stage_song s
    WHERE
        e.song = s.title AND
        e.artist = s.artist_name AND
        ABS(e.length - s.duration) < 2
""")

user_table_insert = ("""
    SELECT DISTINCT (user_id)
        user_id         as u_user_id,
        first_name      as u_first_name,
        last_name       as u_last_name,
        gender          as u_gender,
        level           as u_level
    INTO user
    FROM stage_event
""")

song_table_insert = ("""
    SELECT DISTINCT (song_id)
        song_id     as s_song_id,
        title       as s_title,
        artist_id   as s_artist_id,
        year        as s_year,
        duration    as s_duration
    INTO song
    FROM stage_song
""")

artist_table_insert = ("""
    SELECT DISTINCT (artist_id)
        artist_id           as a_artist_id,
        artist_name         as a_name,
        artist_location     as a_location,
        artist_latitude     as a_latitude,
        artist_longitude    as a_longitude
    INTO artist
    FROM stage_song
""")

time_table_insert = ("""
    SELECT DISTINCT (ts)
        ts                          as t_start_time,
        extract(hour from ts)       as t_hour,
        extract(day from ts)        as t_day,
        extract(week from ts)       as t_week,
        extract(month from ts)      as t_month,
        extract(year from ts)       as t_year,
        extract(weekday from ts)    as t_weekday
    ÍNTO time
    FROM stage_event
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
