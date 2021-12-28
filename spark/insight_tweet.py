import argparse
from posixpath import join
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum, to_date, date_format, countDistinct


#Exporting the data to gold stage of data lake
def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)

#Getting only the parts of interest in the silver stage of data lake
    #using pyspark.sql.functions to manipulate spark dataframe
def transform_gold(spark, src, dest, process_date):
    tweet = spark.read.json(src)
    
    bloomberg = tweet\
    .where("author_id = '34713362'")\
    .select("author_id", "conversation_id")
    
    tweet = tweet.alias("tweet")\
    .join(
        bloomberg.alias("bloomberg"),
        [
            bloomberg.author_id != tweet.author_id,
            bloomberg.conversation_id == tweet.conversation_id
        ],
        'left')\
    .withColumn(
        "bloomberg_conversation",
        when(col("bloomberg.conversation_id").isNotNull(), 1).otherwise(0)
    ).withColumn(
        "reply_bloomberg",
        when(col("tweet.in_reply_to_user_id") == '34713362', 1).otherwise(0)
    ).groupBy(to_date("created_at").alias("created_date"))\
    .agg(
        countDistinct("id").alias("n_tweets"),
        countDistinct("tweet.conversation_id").alias("n_conversation"),
        sum("bloomberg_conversation").alias("bloomberg_conversation"),
        sum("reply_bloomberg").alias("reply_bloomberg")
    ).withColumn("weekday", date_format("created_date", "E"))\
    
    table_dest = join(dest, "{table_name}", f"process_date={process_date}")
    
    export_json(tweet, table_dest.format(table_name="tweet"))

if __name__ == "__main__":
    #only for test
    parser = argparse.ArgumentParser(
        description="Spark Twitter Gold Transformation"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()
    
    spark = SparkSession\
        .builder\
        .appName("twitter_insight_tweet")\
        .getOrCreate()
    
    transform_gold(spark, args.src, args.dest, args.process_date)