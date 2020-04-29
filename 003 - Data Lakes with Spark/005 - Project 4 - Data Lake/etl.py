import os
import configparser
import pyspark.sql as pysql
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session() -> pysql.SparkSession:
    """
    Creates a SparkSession
    Returns: SparkSession
    """
    return pysql.SparkSession.builder.config("spark.jars.packages",
                                              "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()


def load_data(*, spark: pysql.SparkSession, s3_path: str) -> {str: pysql.DataFrame, ...:...}:
    """
    Loads project required data into memory, enforcing a predefined data schema
    Args:
        spark: valid SparkSession, where to execute the code
        s3_path: AWS S3 Bucket path, from where to extract the data

    Returns:
        song_data: metadata about a song and the artist of that song
        log_data: simulate app activity logs from an imaginary music streaming app
    """
    data = {}
    
    song_schema = pysql.types.StructType([
        pysql.types.StructField("num_songs", pysql.types.IntegerType()),
        pysql.types.StructField("artist_id", pysql.types.StringType()),
        pysql.types.StructField("artist_latitude", pysql.types.FloatType()),
        pysql.types.StructField("artist_longitude", pysql.types.FloatType()),
        pysql.types.StructField("artist_location", pysql.types.StringType()),
        pysql.types.StructField("artist_name", pysql.types.StringType()),
        pysql.types.StructField("song_id", pysql.types.StringType()),
        pysql.types.StructField("title", pysql.types.StringType()),
        pysql.types.StructField("duration", pysql.types.FloatType()),
        pysql.types.StructField("year", pysql.types.IntegerType())
    ])

    log_schema = pysql.types.StructType([
        pysql.types.StructField("artist", pysql.types.StringType()),
        pysql.types.StructField("auth", pysql.types.StringType()),
        pysql.types.StructField("first_name", pysql.types.StringType()),
        pysql.types.StructField("gender", pysql.types.StringType()),
        pysql.types.StructField("item_in_session", pysql.types.IntegerType()),
        pysql.types.StructField("last_name", pysql.types.StringType()),
        pysql.types.StructField("length", pysql.types.FloatType()),
        pysql.types.StructField("level", pysql.types.StringType()),
        pysql.types.StructField("location", pysql.types.StringType()),
        pysql.types.StructField("method", pysql.types.StringType()),
        pysql.types.StructField("page", pysql.types.StringType()),
        pysql.types.StructField("registration", pysql.types.FloatType()),
        pysql.types.StructField("session_id", pysql.types.IntegerType()),
        pysql.types.StructField("song", pysql.types.StringType()),
        pysql.types.StructField("status", pysql.types.IntegerType()),
        pysql.types.StructField("ts", pysql.types.TimestampType()),
        pysql.types.StructField("user_agent", pysql.types.StringType()),
        pysql.types.StructField("user_id", pysql.types.IntegerType())
    ])

    data['song_data'] = spark.read.json(s3_path + 'song_data/*/*/*/*.json', schema=song_schema)
    log_data = spark.read.json(s3_path + 'log-data/*.json', schema=log_schema)
    data['log_data'] = log_data.filter(log_data.page == 'NextSong')
    
    return data


def process_song_data(*, song_data: pysql.DataFrame, output_folder: str) -> None:
    """
    This function processes the song data and saves the resulting derived tables in
    parquet format:
        - songs: songs in music database >> song_id, title, artist_id, year, duration
        - artists: artists in music database >> artist_id, name, location, lattitude, longitude

    Args:
        song_data: metadata about a song and the artist of that song
        output_folder: where to save the different output tables in parquet format

    Returns: None
    """
    df_songs = song_data.select('song_id', 'title', 'artist_id', 'year', 'duration')
    df_songs.write.parquet(path=os.path.join(output_folder, 'work', 'data', 'songs.parquet'),
                           partitionBy=['year', 'artist_id'])

    df_artists = song_data \
        .select('artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude')\
        .withColumnRenamed('artist_name', 'name') \
        .withColumnRenamed('artist_location', 'location') \
        .withColumnRenamed('artist_latitude', 'latitude') \
        .withColumnRenamed('artist_longitude', 'longitude')
    df_artists.write.parquet(path=os.path.join(output_folder, 'work', 'data', 'artists.parquet'))


def process_log_data(*, song_data: pysql.DataFrame, log_data: pysql.DataFrame,
                     output_folder: str) -> None:
    """
    This function processes the log data and saves the resulting derived tables in
    parquet format:
        - users: users in the app >> user_id, first_name, last_name, gender, level
        - time: timestamps of records >> start_time, hour, day, week, month, year, weekday
        - songplays: records in log data associated with song plays >> songplay_id, start_time,
                     user_id, level, song_id, artist_id, session_id, location, user_agent

    Args:
        song_data: metadata about a song and the artist of that song
        log_data: simulate app activity logs from an imaginary music streaming app
        output_folder: where to save the different output tables in parquet format

    Returns: None
    """
    df_users = log_data.select('user_id', 'first_name', 'last_name', 'gender', 'level')
    df_users.write.parquet(path=os.path.join(os.getcwd(), 'work', 'data', 'users.parquet'))

    df_time = log_data.select('ts').withColumnRenamed('ts', 'start_time')
    df_time = df_time.withColumn('hour', F.hour(df_time.start_time)) \
        .withColumn('day', F.dayofmonth(df_time.start_time)) \
        .withColumn('week', F.weekofyear(df_time.start_time)) \
        .withColumn('month', F.month(df_time.start_time)) \
        .withColumn('year', F.year(df_time.start_time)) \
        .withColumn('weekday', F.dayofweek(df_time.start_time))
    df_time.write.parquet(path=os.path.join(os.getcwd(), 'work', 'data', 'time.parquet'),
                          partitionBy=['year', 'month'])

    # read in song data to use for songplays table # TODO: DOING
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_folder = ""

    data = load_data(spark=spark, s3_path=input_data)

    process_song_data(song_data=data['song_data'], output_folder=output_folder)

    process_log_data(song_data=data['song_data'], log_data=data['log_data'],
                     output_folder=output_folder)


if __name__ == "__main__":
    main()
