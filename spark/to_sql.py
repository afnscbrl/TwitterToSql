import io
import os
import psycopg2
import pandas as pd
import argparse

from pyspark.sql.session import SparkSession


#Getting the password in system
db_pass = os.environ.get("DB_PASS")

#This function connect with database, get the gold data in data lake and send to postgres database
def to_sql(spark, src):
    conn = psycopg2.connect(host='localhost', port='5432', database='postgres', user='postgres', password=db_pass)
    table = 'tweets'
    df_spark = spark.read.json(src)
    df = df_spark.toPandas()
    columns = tuple(df.columns.values)
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header = False, index = False)
    output.seek(0)
    try:
        cur.copy_from(output, table, null = "", columns = columns)
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()

if __name__ == "__main__":
    #Only for test
    parser = argparse.ArgumentParser(
    description="Data Gold To Sql"
    )
    parser.add_argument("--src", required=True)
    args = parser.parse_args()
    
    spark = SparkSession\
    .builder\
    .appName("gold_to_sql")\
    .getOrCreate()

    to_sql(spark, args.src)