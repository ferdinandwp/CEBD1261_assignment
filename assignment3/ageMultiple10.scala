package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

// This is code for Friends by age dataset
def parseLine(line: String) = {
    val fields = line.split(",")     // CSV file => split by comma
    val age = fields(2).toInt   
    val numFriends = fields(3).toInt       // Convert age to integer
    //val numFriends = fields(3).toInt // Convert number of friends to integer
    (age, numFriends)
}

// Set logger
Logger.getLogger("org").setLevel(Level.ERROR);
// Set config
val conf = new  SparkConf().setMaster("local[*]").setAppName("FriendsByAge").set("spark.driver.host", "localhost")
val sc = new SparkContext(conf)

val lines = sc.textFile("/Users/ferdinand/Desktop/CEBD1261/exercise/fakefriends.csv")

val rdd = lines.map(parseLine)

val groupedAge = rdd.map(x => (x._1/10 * 10, x._2))

val totalGroupedAge = groupedAge.mapValues(x => (x,1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

val averageGroupedAge = totalGroupedAge.mapValues(x => x._1/x._2)

val sortedGroupedAge = averageGroupedAge.sortByKey().collect()

sortedGroupedAge.foreach(println)

