from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[4]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

# df = spark.read.format("csv").option("header", "true").load("csvfile.csv")

# RDD
# .map(lambda f: f.split(","))\
# .map(lambda x: (x[1], list(x[2:])))

data =  spark.read.csv("./res/*.csv") \
    .withColumnRenamed('_c0', 'text')\
    .withColumnRenamed('_c1', 'datetime')\
    .withColumnRenamed('_c2', 'polarity')\
    .withColumnRenamed('_c3', 'subjectivity')\
    .groupBy('datetime')\
    .agg(F.mean('polarity'), F.mean('subjectivity')).show(n=100)

# data.show(n=10000)