import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Creates and returns an spark session
    Arguments:
        none
    Returns:
        the spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Process songs data to create songs and artists table
    Arguments:
        spark: SparkSession object
        input_data: path where to find songs data (Ex: "s3a://udacity-dend/")
        output_data: path where to create the output tables-like files
    Returns:
        nothing
    """
    
    #
    # Deal with songs data, json files with this schema (Example row):
    # {
    # "num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, 
    # "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", 
    # "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0
    #    }
    #
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data files
    df = spark.read.json(song_data)

    
    # Songs table CREATION.
    # song_id, title, artist_id, year, duration

    songs_table = (
        df.select(
            'song_id', 'title', 'artist_id','year', 'duration'
        ).distinct()
    ) 

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs/", mode="overwrite")

    
    # artists table CREATION
    # artists - artists in music database
    # artist_id, name, location, lattitude, longitude

    artists_table = (
        df.select(
            'artist_id',
            col('artist_name').alias('name'),
            col('artist_location').alias('location'),
            col('artist_latitude').alias('lattitude'),
            col('artist_longitude').alias('longitude'),
        ).distinct()
    )
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")
    

def process_log_data(spark, input_data, output_data):
    """ Process logs data to create users, time_table and songplays tables
    Arguments:
        spark: SparkSession object
        input_data: path where to find songs data (Ex: "s3a://udacity-dend/")
        output_data: path where to create the output tables-like files
    Returns:
        The sum of the two arguments
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*"

    # read log data file
    df = spark.read.json(log_data)

    # Fact Table
    # songplays - records in log data associated with song plays i.e. records with page NextSong
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table
    # users - users in the app
    # user_id, first_name, last_name, gender, level
    users_table = (
        df.select(
            col('userId').alias('user_id'),
            col('firstName').alias('first_name'),
            col('lastName').alias('last_name'),
            col('gender').alias('gender'),
            col('level').alias('level')
        ).distinct()
    )
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/', mode='overwrite')

        
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(
        'timestamp',
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        date_format('datetime', 'F').alias('weekday')
    )
    
    
    #songplays - records in log data associated with song plays i.e. records with page NextSong
    #songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data+'time/', mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/")

    # extract columns from joined song and log datasets to create songplays table
    
    df = df.withColumn("songplay_id", monotonically_increasing_id())
    df2 = df.join(song_df, song_df.title == df.song)
    
    songplays_table = (
        df2.select(
            'songplay_id',
            col('timestamp').alias('start_time'),
            col('userId').alias('user_id'),
            'level',
            'song_id',
            'artist_id',
            col('sessionId').alias('session_id'),
            'location',
            col('userAgent').alias('user_agent')
          )
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data + "songplays/", mode="overwrite") 


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://Raul_DataLakes"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
