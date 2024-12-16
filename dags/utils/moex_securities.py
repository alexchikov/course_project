from pyspark import SparkConf
from pyspark.sql import SparkSession
from settings import Settings
import os
import json
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--filename", help="Name of target file")
args = parser.parse_args()

with open(f'{os.environ["HOME"]}/data/{args.filename}') as file:
    data = json.load(file)

result_data = data['boards']

config = SparkConf().set("spark.app.name", "etl_moex_securities") \
    .set("spark.master", "local[*]") \
    .set('spark.driver.extraClassPath',
         f'{os.environ["HOME"]}/dags/jars/postgresql-42.6.1.jar') \
    .set("spark.jars", f"{os.environ['HOME']}/dags/jars/postgresql-42.6.1.jar") \
    .set("spark.jars.packages","org.postgresql:postgresql:42.6.1")
spark = (SparkSession.builder.master("local[*]")
         .config(conf=config)
         .getOrCreate())

securities = spark.sparkContext.parallelize(list(map(tuple, [x for x in result_data['data']])))
sdf_securities = securities.toDF(result_data['columns'])

sdf_securities.write.format("jdbc") \
    .option("url", Settings.JDBC_CONNECTION_URL) \
    .option("user", Settings.JDBC_USER) \
    .option("password", Settings.JDBC_PASSWORD) \
    .option("dbtable", f"moex.{Settings.JDBC_TABLE_SECURITIES}") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
