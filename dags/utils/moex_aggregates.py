import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.connect.functions import to_timestamp
from sqlalchemy.future import select

from settings import Settings
import logging
import os
import json
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--filename", help="Name of target file")
args = parser.parse_args()

with open(f'{os.environ["HOME"]}/data/{args.filename}') as file:
    data = json.load(file)

result_data = data['aggregates']

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

    aggregates = spark.sparkContext.parallelize(list(map(tuple, [x for x in result_data['data']])))
    sdf_aggregates = aggregates.toDF(result_data['columns'])

    sdf_aggregates = sdf_aggregates.withColumn("report_date",
                                               f.date_trunc("YYYY-mm-dd", f.col("updated_at"))) \
        .select(
            "market_name",
            "market_title",
            "engine",
            f.to_date("tradedate").alias("tradedate"),
            "secid",
            "value",
            "volume",
            "numtrades",
            f.to_timestamp("updated_at").alias("updated_at"),
            "report_date"
        )

    sdf_aggregates.write.format("jdbc") \
        .partitionBy("report_date") \
        .option("url", Settings.JDBC_CONNECTION_URL) \
        .option("user", Settings.JDBC_USER) \
        .option("password", Settings.JDBC_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", f"moex.{Settings.JDBC_TABLE_AGGREGATES}") \
        .mode("append") \
        .save(f"moex.{Settings.JDBC_TABLE_AGGREGATES}")
