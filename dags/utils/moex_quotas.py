from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from settings import Settings
import logging
import os
import json
import argparse  #


parser = argparse.ArgumentParser()
parser.add_argument("--filename", help="Name of target file")
args = parser.parse_args()

with open(f'{os.environ["HOME"]}/data/{args.filename}') as file:
    data = json.load(file,
                     parse_int=False,
                     parse_float=False)

result_data = data['history']

if not result_data['data']:
    logging.warning("irrelevant data")
else:
    logging.info("got data successfully")
    config = SparkConf().set("spark.app.name", "etl_moex_securities") \
        .set("spark.master", "local[*]") \
        .set('spark.driver.extraClassPath',
             'dags/jars/postgresql-42.6.1.jar') \
        .set("spark.jars", "dags/jars/postgresql-42.6.1.jar")

    spark = (SparkSession.builder.master("local[*]")
             .config(conf=config)
             .getOrCreate())

    schema = StructType([
        StructField("boardid", StringType(), True),
        StructField("tradedate", StringType(), True),
        StructField("shortname", StringType(), True),
        StructField("secid", StringType(), True),
        StructField("numtrades", StringType(), True),
        StructField("value", StringType(), True),
        StructField("open", StringType(), True),
        StructField("low", StringType(), True),
        StructField("high", StringType(), True),
        StructField("legalcloseprice", StringType(), True),
        StructField("waprice", StringType(), True),
        StructField("close", StringType(), True),
        StructField("volume", StringType(), True),
        StructField("marketprice2", StringType(), True),
        StructField("marketprice3", StringType(), True),
        StructField("admittedquote", StringType(), True),
        StructField("mp2valtrd", StringType(), True),
        StructField("marketprice3tradesvalue", StringType(), True),
        StructField("admittedvalue", StringType(), True),
        StructField("waval", StringType(), True),
        StructField("tradingsession", StringType(), True),
        StructField("currencyid", StringType(), True),
        StructField("trendclspr", StringType(), True)
    ])

    history = spark.sparkContext.parallelize(list(map(tuple, [x for x in result_data['data']])))
    sdf_history = spark.createDataFrame(history, schema)

    sdf_history.write.format("jdbc") \
        .option("url", Settings.JDBC_CONNECTION_URL) \
        .option("user", Settings.JDBC_USER) \
        .option("password", Settings.JDBC_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", f"moex.{Settings.JDBC_TABLE_HISTORY}") \
        .mode("append") \
        .save(f"moex.{Settings.JDBC_TABLE_HISTORY}")
