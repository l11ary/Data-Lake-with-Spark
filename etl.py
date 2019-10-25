import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, DecimalType, LongType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['aws']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def get_song_schema():
    """
    Create a schema to use for the song data.
    :return: StructType object 
    """
    schema = StructType([
            StructField('artist_id', StringType(), True),
            StructField('artist_latitude', DecimalType(), True),
            StructField('artist_longitude', DecimalType(), True),
            StructField('artist_location', StringType(), True),
            StructField('artist_name', StringType(), True),
            StructField('duration', DoubleType(), True),
            StructField('num_songs', IntegerType(), True),
            StructField('song_id', StringType(), True),
            StructField('title', StringType(), True),
            StructField('year', IntegerType(), True)
    ])
    return schema

def get_log_schema():  
    """
    Create a schema to use for the log data.
    :return: StructType object 
    """
    schema = StructType([            
        StructField('artist', StringType(), True),            
        StructField('auth', StringType(), True),            
        StructField('firstName', StringType(), True),            
        StructField('gender', StringType(), True),            
        StructField('itemInSession', LongType(), True),            
        StructField('lastName', StringType(), True),            
        StructField('length', DoubleType(), True),        
        StructField('level', StringType(), True),            
        StructField('location', StringType(), True),            
        StructField('method', StringType(), True),            
        StructField('page', StringType(), True),            
        StructField('registration', DoubleType(), True),         
        StructField('sessionId', LongType(), True),            
        StructField('song', StringType(), True),            
        StructField('status', LongType(), True),            
        StructField('ts', LongType(), True),            
        StructField('userAgent', StringType(), True),            
        StructField('userId', StringType(), True)        
     ])   
    return schema

def process_song_data(spark, input_data, output_data):
    """
    process song dataset, extract songs and artists table and write them to the output path
    """
        
    # get filepath to song data file
    song_data =   "song_data/*/*/*/*.json"

    # read song data file
    songSchema = get_song_schema()
    df = spark.read.json(song_data, schema=songSchema)
    
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates(['song_id'])
    songs_table.limit(5).toPandas()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.parquet(output_data + "songs_table.parquet", mode="overwrite")

    import pdb;pdb.set_trace()
    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", \
                                  "artist_longitude as lattitude", "artist_longitude as longitude").dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table.parquet", mode="overwrite")



def process_log_data(spark, input_data, output_data):
    """
    process log dataset, extract users, time and songplays table and write them to the output path
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"

    # read log data file
    logSchema = get_log_schema()
    df = spark.read.json(log_data, schema=logSchema)
        
    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").dropDuplicates(['user_id'])
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users_table.parquet", partitionBy=["year", "month"], mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000)), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x/1000)), TimestampType())
    df = df.withColumn("datetime", get_datetime(col("ts")))

    
    # extract columns to create time table
    time_table = df.selectExpr("timestamp as start_time", \
                               "hour(timestamp) as hour", \
                               "dayofmonth(timestamp) as day", \
                               "weekofyear(timestamp) as week", \
                               "month(timestamp) as month", \
                               "year(timestamp) as year", 
                               "dayofweek(timestamp) as weekday").dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time_table.parquet", partitionBy=["year", "month"], mode="overwrite")

    # read in song data to use for songplays table
    song_data = "song_data/*/*/*/*.json"
    songSchema = get_song_schema()
    song_df = spark.read.json(song_data, schema=songSchema)

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("songs")
    df.createOrReplaceTempView("logs")


    songplays_table = spark.sql("""
                            SELECT  l.timestamp as start_time, 
                                    l.userId as user_id,
                                    l.level as level,
                                    s.song_id as song_id,
                                    s.artist_id as artist_id,
                                    l.sessionId as session_id,
                                    l.location as location,
                                    l.userAgent as user_agent,
                                    year(l.timestamp) as year,
                                    month(l.timestamp) as month
                            FROM logs l JOIN songs s ON (l.song=s.title AND l.length=s.duration AND l.artist=s.artist_name)
    """)
   
         
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())
    s_table.limit(5).toPandas()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays_table.parquet", partitionBy=["year", "month"], mode="overwrite")


def test_parquet(spark, output_data):
    """
    test the final data
    """
    
    users = spark.read.parquet(output_data + "users_table.parquet")
    artists = spark.read.parquet(output_data + "artists_table.parquet")
    time = spark.read.parquet(output_data + "time_table.parquet")
    songs = spark.read.parquet(output_data + "songs_table.parquet")
    songplays = spark.read.parquet(output_data + "songplays_table.parquet")
    

    print ("Number of records in users table: ", users.count())
    print ("Number of records in artists table: ", artists.count())
    print ("Number of records in time table: ", time.count())
    print ("Number of records in songs table: ", songs.count())
    print ("Number of records in songplays table: ", songplays.count())

    print("display maximum 5 records where the songs are played in washington")
    songplays.createOrReplaceTempView("songplays")
    spark.sql("select * from songplays where location like '%Washington%'").limit(5).toPandas()
    
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-project/datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    test_parquet(spark, output_data)

if __name__ == "__main__":
    main()
