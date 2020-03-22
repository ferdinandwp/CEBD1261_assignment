// Hotel Booking Demand dataset from Kaggle
// https://www.kaggle.com/jessemostipak/hotel-booking-demand
 
// Import libraries
package com.cellariot.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import scala.math.max

def parseLine(line: String) = {
    val fields = line.split(",")
    val hotel = fields(0)
    val isCancelled = fields(1)
    val leadTime = fields(2)
    (hotel, isCancelled, leadTime)
}
Logger.getLogger("org").setLevel(Level.ERROR)
val conf = new  SparkConf().setMaster("local[*]").setAppName("Hotel_Booking").set("spark.driver.host", "localhost");
val sc = new SparkContext(conf)
val lines = sc.textFile("/Users/ferdinand/Desktop/CEBD1261_assignment/project/hotel_bookings.csv")
val rdd = lines.map(parseLine)