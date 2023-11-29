from certifi.__main__ import args
from pyspark import SparkContext, SparkConf
import urllib
from urllib import request
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import requests


def main():
  conf = SparkConf().setAppName("pyspark_project").setMaster("local[*]")
  sc = SparkContext(conf=conf)
  sc.setLogLevel("ERROR")

  spark = SparkSession.builder.getOrCreate()

  #loading the data from S3
  configdf = spark.read.format("json").option("multiline", "true").load("s3://azeyodev/config.json")
  configdf.show(10)

  # Select the 'dev' environment
  devdf = configdf.select("env.dev.*")
  devdf.show(10)

  # Extract source, avail, and notavail values
  src = devdf.select("src").rdd.map(lambda x: x[0]).collect()[0]
  avail = devdf.select("avail").rdd.map(lambda x: x[0]).collect()[0]
  notavail = devdf.select("notavail").rdd.map(lambda x: x[0]).collect()[0]

  # Read Avro data from source
  data = spark.read.format("avro").load(src)
  data.show(10)

  #download data from web url
  url = "https://randomuser.me/api/0.8/?results=500"
  response = requests.get(url)

  # Check if the request was successful (status code 200)
  if response.status_code == 200:
      # Extract the content of the response
      s = response.text

      # Convert responce to rdd and then to dataframe
      urldf = spark.read.json(spark.sparkContext.parallelize([s]))
      urldf.show(10)
      urldf.printSchema()

      # Explode and select columns
      flatdf = urldf.withColumn("results", explode(col("results"))).select(
        "nationality", "seed", "version",
        "results.user.username", "results.user.cell", "results.user.dob", "results.user.email",
        "results.user.gender", "results.user.location.city", "results.user.location.state",
        "results.user.location.street", "results.user.location.zip", "results.user.md5",
        "results.user.name.first", "results.user.name.last", "results.user.name.title",
        "results.user.password", "results.user.phone", "results.user.picture.large",
        "results.user.picture.medium", "results.user.picture.thumbnail", "results.user.registered",
        "results.user.salt", "results.user.sha1", "results.user.sha256"
      )

      # Replace digits in the 'username' column
      rm = flatdf.withColumn("username", regexp_replace(col("username"), "[0-9]", ""))
      rm.show(10)

      # Join Avro data with downloaded data
      joindf = data.join(rm, ["username"], "left")
      joindf.show(20)

      # Filter rows with null and not null 'nationality'
      dfnull = joindf.filter(col("nationality").isNull)
      dfnotnull = joindf.filter(col("nationality").isNotNull)
      dfnotnull.show(20)
      dfnull.show(20)

      # Replace null values and fill with defaults
      replacenull = dfnull.na.fill("Not Available").na.fill(0)
      replacenull.show()

      # Add a 'current_date' column
      noavail = replacenull.withColumn("current_date", current_date())
      avail1 = dfnotnull.withColumn("current_date", current_date())

      # Write to Parquet format
      avail1.write.format("parquet").mode("overwrite").save(f"{avail}/{args[1]}")
      noavail.write.format("parquet").mode("overwrite").save(f"{notavail}/{args[1]}")

  else:
      print(f"Failed to retrieve data from URL. Status code: {response.status_code}")


if __name__ == '__main__':
  main()