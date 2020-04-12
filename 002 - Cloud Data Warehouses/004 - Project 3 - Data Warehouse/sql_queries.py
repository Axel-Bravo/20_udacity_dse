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
                                     user_id            INTEGER
                                     );
                              """)

staging_songs_table_create = ("""
                                CREATE TABLE IF NOT EXISTS staging_songs (
                                    num_songs        INTEGER,
                                    artist_id        VARCHAR(50),
                                    artist_latitude  FLOAT,
                                    artist_longitud  FLOAT,
                                    artist_location  VARCHAR(100),
                                    artist_name      VARCHAR(100),
                                    song_id          VARCHAR(50) SORTKEY,
                                    title            VARCHAR(150),
                                    duration         FLOAT,
                                    year             INTEGER
                                    );
                             """)

songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id     VARCHAR SORTKEY,
                                start_time      BIGINT NOT NULL DISTKEY,
                                user_id         INTEGER NOT NULL,
                                level           VARCHAR(15) NOT NULL,
                                song_id         VARCHAR(25) NOT NULL,
                                artist_id       VARCHAR(25) NOT NULL,
                                session_id      INTEGER NOT NULL,
                                location        VARCHAR(100) NOT NULL,
                                user_agent      VARCHAR(75) NOT NULL
                             );
                        """)

user_table_create = ("""
                            CREATE TABLE IF NOT EXISTS users (
                                user_id     INTEGER SORTKEY,
                                first_name  VARCHAR(30) NOT NULL,
                                last_name   VARCHAR(30) NOT NULL,
                                gender      VARCHAR(1) NOT NULL,
                                level       VARCHAR(15) NOT NULL
                            );
                    """)

song_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songs (
                                song_id    VARCHAR(25) SORTKEY,
                                title      VARCHAR(50) NOT NULL,
                                artist_id  VARCHAR(25) NOT NULL,
                                year       INTEGER NOT NULL,
                                duration   FLOAT NOT NULL
                            );
                    """)

artist_table_create = ("""
                            CREATE TABLE IF NOT EXISTS artists (
                                artist_id     VARCHAR(25) SORTKEY, 
                                name          VARCHAR(50) NOT NULL,
                                location      VARCHAR(100) NOT NULL,
                                latitude      FLOAT,
                                longitude     FLOAT
                            );
                       """)

time_table_create = ("""
                            CREATE TABLE IF NOT EXISTS time (
                                start_time  TIMESTAMP SORTKEY,
                                hour        INTEGER NOT NULL,
                                day         INTEGER NOT NULL,
                                week        INTEGER NOT NULL,
                                month       INTEGER NOT NULL,
                                year        INTEGER NOT NULL,
                                weekday     INTEGER NOT NULL
                            );
                     """)


# STAGING TABLES
staging_events_copy = ("""
                            COPY staging_events
                            FROM '{}'
                            IAM_ROLE '{}'
                            REGION 'us-west-2'
                            FORMAT AS JSON '{}' 
                            TIMEFORMAT 'epochmillisecs';
                      """).format(config['S3']['LOG_DATA'],
                                  config['IAM_ROLE']['ARN'],
                                  config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
                           COPY staging_songs 
                           FROM '{}'
                           IAM_ROLE '{}'
                           REGION 'us-west-2'
                           FORMAT AS JSON 'auto';
                      """).format(config['S3']['SONG_DATA'],
                                  config['IAM_ROLE']['ARN'],)


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
create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert,
                        artist_table_insert, time_table_insert]
