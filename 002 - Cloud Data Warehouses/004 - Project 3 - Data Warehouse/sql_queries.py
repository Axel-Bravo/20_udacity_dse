import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES
staging_events_table_create = ("""
                                 CREATE TABLE IF NOT EXISTS staging_events (
                                     artist             VARCHAR(150),
                                     auth               VARCHAR(50),
                                     first_name         VARCHAR(50),
                                     gender             VARCHAR(1),
                                     item_in_session    INTEGER,
                                     last_name          VARCHAR(50),
                                     length             FLOAT,
                                     level              VARCHAR(50),
                                     location           VARCHAR(100),
                                     method             VARCHAR(50),
                                     page               VARCHAR(50),
                                     registration       FLOAT,
                                     session_id         INTEGER,
                                     song               VARCHAR(200),
                                     status             INTEGER,
                                     ts                 TIMESTAMP,
                                     user_agent         VARCHAR(150) ,
                                     user_id            INTEGER DISTKEY
                                     );
                              """)

staging_songs_table_create = ("""
                                CREATE TABLE IF NOT EXISTS staging_songs (
                                    num_songs        INTEGER,
                                    artist_id        VARCHAR(50),
                                    artist_latitude  FLOAT,
                                    artist_longitud  FLOAT,
                                    artist_location  VARCHAR(250),
                                    artist_name      VARCHAR(250),
                                    song_id          VARCHAR(50) SORTKEY DISTKEY,
                                    title            VARCHAR(250),
                                    duration         FLOAT,
                                    year             INTEGER
                                    );
                             """)

songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id     INT IDENTITY(0, 1) PRIMARY KEY SORTKEY,
                                start_time      TIMESTAMP DISTKEY NOT NULL ,
                                user_id         INTEGER NOT NULL ,
                                level           VARCHAR(50),
                                song_id         VARCHAR(50) NOT NULL ,
                                artist_id       VARCHAR(25) NOT NULL ,
                                session_id      INTEGER,
                                location        VARCHAR(100),
                                user_agent      VARCHAR(150) 
                             );
                        """)

user_table_create = ("""
                            CREATE TABLE IF NOT EXISTS users (
                                user_id     INTEGER PRIMARY KEY SORTKEY DISTKEY,
                                first_name  VARCHAR(50),
                                last_name   VARCHAR(50),
                                gender      VARCHAR(1),
                                level       VARCHAR(50) 
                            );
                    """)

song_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songs (
                                song_id    VARCHAR(50) PRIMARY KEY SORTKEY,
                                title      VARCHAR(250),
                                artist_id  VARCHAR(50),
                                year       INTEGER DISTKEY,
                                duration   FLOAT 
                            );
                    """)

artist_table_create = ("""
                            CREATE TABLE IF NOT EXISTS artists (
                                artist_id     VARCHAR(50) PRIMARY KEY SORTKEY DISTKEY, 
                                name          VARCHAR(250),
                                location      VARCHAR(250),
                                latitude      FLOAT,
                                longitude     FLOAT
                            );
                       """)

time_table_create = ("""
                            CREATE TABLE IF NOT EXISTS time (
                                start_time  TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
                                hour        INTEGER,
                                day         INTEGER,
                                week        INTEGER,
                                month       INTEGER,
                                year        INTEGER,
                                weekday     INTEGER 
                            );
                     """)


# STAGING TABLES
staging_events_copy = ("""
                            COPY staging_events
                            FROM {}
                            IAM_ROLE {}
                            REGION 'us-west-2'
                            FORMAT AS JSON {} 
                            TIMEFORMAT 'epochmillisecs';
                      """).format(config['S3']['LOG_DATA'],
                                  config['IAM_ROLE']['ARN'],
                                  config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
                           COPY staging_songs 
                           FROM {}
                           IAM_ROLE {}
                           REGION 'us-west-2'
                           FORMAT AS JSON 'auto';
                      """).format(config['S3']['SONG_DATA'],
                                  config['IAM_ROLE']['ARN'],)


# FINAL TABLES
songplay_table_insert = ("""
                            INSERT INTO songplays (start_time, user_id, level, song_id, 
                                           artist_id, session_id, location, user_agent)
                                           
                            SELECT events.ts,
                                   events.user_id,
                                   events.level,
                                   songs.song_id,
                                   songs.artist_id,
                                   events.session_id,
                                   events.location,
                                   events.user_agent
                                   
                            FROM staging_songs AS songs
                            JOIN staging_events AS events ON events.song = songs.title
                                                          AND events.artist = songs.artist_name
                            WHERE songs.song_id IS NOT NULL
                            AND songs.artist_id IS NOT NULL 
                            AND events.session_id IS NOT NULL 
                            AND events.user_id IS NOT NULL
                            AND events.page = 'NextSong';
""")

user_table_insert = (""" 
                        INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level
                        FROM staging_events
                        WHERE user_id IS NOT NULL
                        AND page = 'NextSong';               
""")

song_table_insert = ("""
                        INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song id,
                                        title,
                                        artist_id,
                                        year,
                                        duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL;
                                        
""")

artist_table_insert = ("""
                          INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id,
                                          artist_name,
                                          artist_location,
                                          artist_latitude,
                                          artist_longitude
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
                        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT start_time,
                                        EXTRACT HOUR FROM start_time,
                                        EXTRACT DAY FROM start_time,
                                        EXTRACT WEEK FROM start_time,
                                        EXTRACT MONTH FROM start_time,
                                        EXTRACT YEAR FROM start_time,
                                        EXTRACT DOW FROM start_time
                        FROM songplays
                        WHERE songplays IS NOT NULL;
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert,
                        artist_table_insert, time_table_insert]
