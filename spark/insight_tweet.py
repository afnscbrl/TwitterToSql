from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import when, col, sum, to_date, date_format, countDistinct

def  get_tweets():
    pass

def get_user():
    pass

def data_gold():
    tweet = spark.read.json(
        "/home/afnscbrl/Documents/TwitterToSQL/datalake/Silver/bloomberg/tweet"
    )

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
    
    tweet.coalesce(1)\
        .write\
        .mode("overwrite")\
        .json(
        "/home/afnscbrl/Documents/TwitterToSQL/datalake/Gold"
    )


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("twitter_insight_tweet")\
        .getOrCreate()
    
    tweet = spark.read.json(
        "/home/afnscbrl/Documents/TwitterToSQL/datalake/Silver/bloomberg/tweet"
    )

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
    
    tweet.coalesce(1)\
        .write\
        .mode("overwrite")\
        .json(
        "/home/afnscbrl/Documents/TwitterToSQL/datalake/Gold"
    )