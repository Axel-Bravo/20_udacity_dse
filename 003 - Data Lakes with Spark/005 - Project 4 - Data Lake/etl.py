import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']= config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark: SparkSession, s3_path: str, output_folder: str) -> None:
    """
    This function processes the song data, in JSON format, and saves the resulting tables in
    parquet format:
        - songs: song_id, title, artist_id, year, duration
        - artists: artist_id, name, location, lattitude, longitude

    Args:
        spark: valid SparkSession, where to execute the code
        s3_path: AWS S3 Bucket path, from where to extract the data
        output_folder: where to save the different output tables in parquet format

    Attributes
        song_data: S3's sub-folder containing 'song data'
        song_schema: JSON's song schema


    Returns: None

    """

    song_data: str = 'song_data'

    song_schema = types.StructType([
        types.StructField("num_songs", types.IntegerType()),
        types.StructField("artist_id", types.StringType()),
        types.StructField("artist_latitude", types.FloatType()),
        types.StructField("artist_longitude", types.FloatType()),
        types.StructField("artist_location", types.StringType()),
        types.StructField("artist_name", types.StringType()),
        types.StructField("song_id", types.StringType()),
        types.StructField("title", types.StringType()),
        types.StructField("duration", types.FloatType()),
        types.StructField("year", types.IntegerType())
    ])

    song_data = spark.read.json(s3_path + song_data + '/*/*/*/*.json', schema=song_schema)

    df_songs = song_data.select('song_id', 'title', 'artist_id', 'year', 'duration')
    df_songs.write.parquet(path=os.path.join(output_folder, 'work', 'data', 'songs.parquet'),
                           partitionBy=['year', 'artist_id'])

    df_artists = song_data\
        .select('artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude')\
        .withColumnRenamed('artist_name', 'name')\
        .withColumnRenamed('artist_location', 'location')\
        .withColumnRenamed('artist_latitude', 'latitude')\
        .withColumnRenamed('artist_longitude', 'longitude')

    df_artists.write.parquet(path=os.path.join(output_folder, 'work', 'data', 'artists.parquet'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
