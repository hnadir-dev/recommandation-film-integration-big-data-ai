import findspark
findspark.init()


from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
SparkSession.builder.config(conf=SparkConf())
from pyspark.sql.types import StructType, StructField, StringType,ArrayType
from pyspark.sql.functions import from_json, col, to_date, from_unixtime, to_utc_timestamp

import logging

kafka_group_id = 'group-id'

kafka_config = {
    'group.id': kafka_group_id,
    "bootstrap.servers": "localhost:9092", 
}


def create_spark_configuration():
    spark_config = None
    try:
        spark_config = (SparkSession.builder
            .appName("ElasticsearchSparkIntegration")
            .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-20_2.12:7.17.14,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4")
            .getOrCreate())
        
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return spark_config

def connect_to_kafka(spark_config,topic):
    spark_df = None
    try:
        spark_df = spark_config.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .load()
        
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_selection_df_movie_from_kafka(spark_df):
    schema = StructType([
        StructField("movieId", StringType(), False),
        StructField("name", StringType(), False),
        StructField("release_date", StringType(), False),
        StructField("url", StringType(), False),
        StructField("genre", ArrayType(StringType()), False)
    ])

    sel = (spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*") \
        .withColumn("movieId", col('movieId').cast('integer')) \
        .withColumn('release_date',to_date(col('release_date'),"dd-MMM-yyyy")) \
        .select('movieId','name','release_date','url','genre'))

    return sel

def create_selection_df_user_from_kafka(spark_df):
    schema = StructType([
        StructField("userId", StringType(), False),
        StructField("age", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("function", StringType(), False),
    ])

    sel = (spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*") \
        .withColumn("userId", col('userId').cast('integer')) \
        .withColumn("age", col('age').cast('integer')) \
        .select('userId','age','gender','function'))

    return sel

def create_selection_df_ratings_from_kafka(spark_df):
    schema = StructType([
        StructField("userId", StringType(), False),
        StructField("movieId", StringType(), False),
        StructField("rating", StringType(), False),
        StructField("timestamp", StringType(), False),
    ])

    sel = (spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*") \
        .withColumn("umid", col('userId').cast('integer') + col('movieId').cast('integer')) \
        .withColumn("userId", col('userId').cast('integer')) \
        .withColumn("movieId", col('movieId').cast('integer')) \
        .withColumn("rating", col('rating').cast('integer')) \
        # to_date(col('timestamp').cast('integer'))
        .withColumn("timestamp", to_utc_timestamp(from_unixtime((col('timestamp').cast('integer')/1000),'yyyy-MM-dd HH:mm:ss'),'EST')) \
        .select('umid','userId','movieId','rating','timestamp'))

    return sel

#Configuration kafka
spark_config = create_spark_configuration()

#DataFrames
df_movies = connect_to_kafka(spark_config,'movies-topic')
df_users = connect_to_kafka(spark_config,'users-topic')
df_ratings = connect_to_kafka(spark_config,'ratings-topic')

#Selections
sel_movies = create_selection_df_movie_from_kafka(df_movies)
sel_users = create_selection_df_user_from_kafka(df_users)
sel_ratings = create_selection_df_ratings_from_kafka(df_ratings)

#Debug
# streaming_es_query = sel_ratings.writeStream.outputMode("append").format("console").option("format", "json").start()

# streaming_es_query.awaitTermination()

# Function to write data to Elasticsearch
def write_to_elasticsearch(df,index,docId):

    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.resource", f"{index}/_doc") \
        .option("es.mapping.id", docId) \
        .save(mode="append")

#Write data to Elasticsearch
write_movies_query = (sel_movies.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/elasticsearch_movies_1/") \
    .foreachBatch(lambda batch_df, batch_id: write_to_elasticsearch(batch_df,'rec-movies-index','movieId')) \
    .start())

write_users_query = (sel_users.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/elasticsearch_users_1/") \
    .foreachBatch(lambda batch_df, batch_id: write_to_elasticsearch(batch_df,'rec-users-index','userId')) \
    .start())

write_ratings_query = (sel_ratings.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/elasticsearch_rating_1/") \
    .foreachBatch(lambda batch_df, batch_id: write_to_elasticsearch(batch_df,'rec-ratings-index','umid')) \
    .start())

write_movies_query.awaitTermination()
write_users_query.awaitTermination()
write_ratings_query.awaitTermination()