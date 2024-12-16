from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from settings import Settings
import sys
import logging
import os
import json
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--filename", help="Name of target file")
args = parser.parse_args()

with open(f'{os.environ["HOME"]}/data/{args.filename}') as file:
    data = json.load(file)

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

    history = spark.sparkContext.parallelize(list(map(tuple, [x for x in result_data['data']])))
    sdf_history = history.toDF(result_data['columns'])

    sdf_history.write.format("jdbc") \
        .option("url", Settings.JDBC_CONNECTION_URL) \
        .option("user", Settings.JDBC_USER) \
        .option("password", Settings.JDBC_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", f"moex.{Settings.JDBC_TABLE_HISTORY}") \
        .mode("append") \
        .save(f"moex.{Settings.JDBC_TABLE_HISTORY}")
