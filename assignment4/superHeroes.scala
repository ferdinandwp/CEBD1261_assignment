package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._    
import org.apache.spark.sql.functions._


def countCoOccurences(line: String) = {
var elements = line.split("\\s+")
( elements(0).toInt, elements.length - 1 )
}

// Function to extract hero ID -> hero name tuples (or None in case of failure)
def parseNames(line: String) : Option[(Int, String)] = {
var fields = line.split('\"')
if (fields.length > 1) {
    return Some(fields(0).trim().toInt, fields(1))
} else {
    return None // flatmap will just discard None results, and extract data from Some results.
}
}

Logger.getLogger("org").setLevel(Level.ERROR)
val conf = new  SparkConf().setMaster("local[*]").setAppName("MostPopularSuperhero").set("spark.driver.host", "localhost");
val sc = new SparkContext(conf) 
val names = sc.textFile("/Users/ferdinand/Desktop/CEBD1261/exercise/marvel-names.txt")
val namesRdd = names.flatMap(parseNames)
val lines = sc.textFile("/Users/ferdinand/Desktop/CEBD1261/exercise/marvel-graph.txt")
val pairings = lines.map(countCoOccurences)
val totalFriendsByCharacter = pairings.reduceByKey( (x,y) => x + y )
val flipped = totalFriendsByCharacter.map( x => (x._2, x._1) )
val flippedSorted = flipped.sortByKey(false)
val namesRDDdf = spark.createDataFrame(namesRdd).toDF("ID","Name")
val top10Sorted = spark.createDataFrame(flippedSorted).toDF("Co-Occurance","ID")
val top10unordered = top10Sorted.limit(10)
val testJoin = top10unordered.join(namesRDDdf,top10unordered("ID") === namesRDDdf("ID"), "left")
val removeId = testJoin.drop("ID")
val top10 = removeId.sort(desc("Co-Occurance"))
top10.show()