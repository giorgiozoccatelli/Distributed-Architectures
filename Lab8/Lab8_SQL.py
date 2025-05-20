import sys
from pyspark import *
from pyspark.sql import SparkSession
from datetime import datetime

inputPath1 = "/data/students/bigdata-01QYD/Lab7_DBD/register.csv"
inputPath2 = "/data/students/bigdata-01QYD/Lab7_DBD/stations.csv"
outputPath = "Lab8/Lab8_SQL" 
threshold = 0.6 

spark = SparkSession.builder.appName("Spark Lab #8").getOrCreate()


inputDF1 = spark.read.load(inputPath1, format="csv", delimiter="\\t", header=True, inferSchema=True)
inputDF1.createOrReplaceTempView("readings")

def fullFunction(free_slots):
    if free_slots==0:
        return 1
    else:
        return 0
    
spark.udf.register("full", fullFunction)

selectedPairsDF = spark.sql("SELECT station, date_format(timestamp,'EE') as dayofweek, hour(timestamp) as hour, avg(full(free_slots)) as criticality\
                            FROM readings\
                            WHERE free_slots <> 0 OR used_slots <> 0\
                            GROUP BY station, date_format(timestamp,'EE'), hour(timestamp)\
                            HAVING avg(full(free_slots))>"+str(threshold))

selectedPairsDF.createOrReplaceTempView("criticals")

inputDF2 = spark.read.load(inputPath2, format="csv",delimiter="\\t",header=True,inferSchema=True)
inputDF2.createOrReplaceTempView("stations")

orderedRes = spark.sql("SELECT station, dayofweek,hour,longitude,latitude,criticality\
                       FROM criticals, stations\
                       WHERE criticals.station = stations.id\
                       ORDER BY criticality DESC")


orderedRes.write.format("csv").option("header", True).save(outputPath)

spark.stop()