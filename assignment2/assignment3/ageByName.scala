package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

// This is code for Friends by age dataset
def parseLine(line: String) = {
    val fields = line.split(",")     // CSV file => split by comma
    val name = fields(1).toString   
    val age = fields(2).toInt       // Convert age to integer
    //val numFriends = fields(3).toInt // Convert number of friends to integer
    (name, age)
}


// Set logger
Logger.getLogger("org").setLevel(Level.ERROR);
// Set config
val conf = new  SparkConf().setMaster("local[*]").setAppName("FriendsByAge").set("spark.driver.host", "localhost");
val sc = new SparkContext(conf);


// Read the file => replace file directory to try the code
val lines = sc.textFile("/Users/ferdinand/Desktop/CEBD1261/exercise/fakefriends.csv")

val rdd = lines.map(parseLine)

val totalByName = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

val averageByName = totalByName.mapValues(x => x._1 / x._2).collect()

averageByName.foreach(println)
