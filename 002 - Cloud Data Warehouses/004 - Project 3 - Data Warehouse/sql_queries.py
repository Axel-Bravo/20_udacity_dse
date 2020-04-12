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
                                    id                 BIGINT IDENTITY(0,1) SORTKEY,
                                    artist             VARCHAR(50),
                                    auth               VARCHAR(25) NOT NULL,
                                    first_name         VARCHAR(30) NOT NULL,
                                    gender             VARCHAR(1) NOT NULL,
                                    intem_in_session   INTEGER  NOT NULL,
                                    last_name          VARCHAR(30) NOT NULL,
                                    lenght             FLOAT NOT NULL,
                                    level              VARCHAR(15) NOT NULL,
                                    location           VARCHAR(100) NOT NULL,
                                    method             VARCHAR(15) NOT NULL,
                                    page               VARCHAR(15) NOT NULL,
                                    registration       TIMESTAMP NOT NULL,
                                    session_id         INTEGER NOT NULL,
                                    status             INTEGER NOT NULL,
                                    ts                 TIMESTAMP NOT NULL,
                                    user_agent         VARCHAR(75) NOT NULL,
                                    user_id            INT NOT NULL
                                    );
                              """)

staging_songs_table_create = ("""
                                CREATE TABLE IF NOT EXISTS staging_songs (
                                    num_songs        INTEGER NOT NULL,
                                    artist_id        VARCHAR(25) NOT NULL,
                                    artist_latitude  FLOAT,
                                    artist_longitud  FLOAT,
                                    artist_location  VARCHAR(50) NOT NULL,
                                    artist_name      VARCHAR(50) NOT NULL,
                                    song_id          VARCHAR(25) NOT NULL SORTKEY,
                                    title            VARCHAR(50) NOT NULL,
                                    duration         FLOAT NOT NULL,
                                    year             INTEGER NOT NULL
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
                                start_time  TIMESTAMP SORTKEY ,
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
""").format()

staging_songs_copy = ("""
""").format()


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
