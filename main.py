from pyspark import SparkContext, SparkConf
import urllib
from urllib import request
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
  conf = SparkConf().setAppName("pyspark_project").setMaster("local[*]")
  sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()



if __name__ == '__main__':
  main()