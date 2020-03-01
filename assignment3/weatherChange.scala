// This script is for weather change
package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import scala.math.max

def parseLine(line: String) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
}


Logger.getLogger("org").setLevel(Level.ERROR)
val conf = new  SparkConf().setMaster("local[*]").setAppName("MinTemperatures").set("spark.driver.host", "localhost");
val sc = new SparkContext(conf)

val lines = sc.textFile("/Users/ferdinand/Desktop/CEBD1261/exercise/1800.csv")
val parsedLines = lines.map(parseLine)

// Create 2 RDD 1 for TMIN and 1 for TMAX
val minTemp = parsedLines.filter(x => x._2 == "TMIN")
val minByStation = minTemp.map(x => (x._1, x._3.toFloat))
val minTempsByStation = minByStation.reduceByKey((x,y) => min(x,y))

val maxTemp = parsedLines.filter(x => x._2 == "TMAX")
val maxByStation = maxTemp.map(x => (x._1, x._3.toFloat))
val maxTempsByStation = maxByStation.reduceByKey((x,y) => min(x,y))

// join max and min temp
val joinTemp = minTempsByStation.join(maxTempsByStation)

// find temperature difference
val tempDifference = joinTemp.mapValues(x => (x._2 - x._1))

tempDifference.collect().foreach(println)








