import sys
from pyspark import *
from pyspark.sql import SparkSession
from datetime import datetime

inputPath1  = "/data/students/bigdata-01QYD/Lab7_DBD/register.csv" 
inputPath2 = "/data/students/bigdata-01QYD/Lab7_DBD/stations.csv" 
threshold  = 0.6 
outputPath = "Lab8/res_Lab_8" 

spark = SparkSession.builder.appName("Spark Lab #7 - Template").getOrCreate()

inputDF1 = spark.read.load(inputPath1, format="csv", delimiter="\\t", header=True, inferSchema=True)
filterDF = inputDF1.filter("free_slots<>0 OR used_slots<>0")

def fullFunction(free_slots):
    if free_slots==0:
        return 1
    else:
        return 0
    
spark.udf.register("full", fullFunction)

stationWeekdayHourDF = filterDF.selectExpr("station", "date_format(timestamp,'EE') as dayofweek", "hour(timestamp) as hour", "full(free_slots) as fullstatus")
groupedStationWeekdayHourDF = stationWeekdayHourDF.groupBy("station", "dayofweek", "hour")
criticalityDF = groupedStationWeekdayHourDF.agg({"fullstatus":"avg"}).withColumnRenamed("avg(fullstatus)","criticality")
                                           
selectedCriticalityDF = criticalityDF.filter("criticality > "+str(threshold))

inputDF2 = spark.read.load(inputPath2, format="csv", delimiter="\\t", header=True, inferSchema=True)
selectedCriticalityCoordinatesDF = selectedCriticalityDF.join(inputDF2,selectedCriticalityDF.station==inputDF2.id)

orderedDF = selectedCriticalityCoordinatesDF.selectExpr("station", "dayofweek", "hour", "longitude", "latitude", "criticality").sort("criticality", ascending=False)
orderedDF.write.format("csv").option("header", True).save(outputPath)

spark.stop()