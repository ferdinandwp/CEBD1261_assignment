// This script is for weather change
package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import scala.math.max
import org.joda.time.{DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.joda.time._
import spark.implicits._
import java.time.LocalDate
import java.time.format.DateTimeFormatter


def parseLine(line: String) ={
    val fields = line.split(",")
    val stationID = fields(0)
    val tempDate = fields(1).toLong
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, tempDate, entryType, temperature)
}

Logger.getLogger("org").setLevel(Level.ERROR)
val conf = new  SparkConf().setMaster("local[*]").setAppName("MinTemperatures").set("spark.driver.host", "localhost");
val sc = new SparkContext(conf)

val lines = sc.textFile("/Users/ferdinand/Desktop/CEBD1261/exercise/1800.csv")

val rdd = lines.map(parseLine)

val rddTrans = rdd.map(x => (x._1, (x._2 * 1000L).toString, x._3, x._4))
val rddDf = rddTrans.toDF("stationID","tempDate", "entryType", "temperature")
