package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


// set logger
Logger.getLogger("org").setLevel(Level.ERROR)
val sc = new SparkContext("local[*]", "WordCount")
val input = sc.textFile("/Users/ferdinand/Desktop/CEBD1261/exercise/book.txt")
//val words1 = input.flatMap(x => x.split(" ")) // split it by space
val words2 = input.flatMap(x => x.split("\\W+")) // split by 
val words3 = input.flatMap(x => x.toLowerCase())
val words4 = words2.map(x => (x,1)).reduceByKey((x,y) => x + y)
val words4sorted = words4.map(x => (x._2, x._1)).sortByKey(false).collect()
//val top10Words = words4sorted.flatMap( _._2.lines.drop(5))
val dfSorted = spark.createDataFrame(words4sorted).toDF("Count","Word")
val indexed = dfSorted.withColumn("index", monotonicallyIncreasingId()) //assign index
val filtered = indexed.filter(col("index") > 6).drop("index") // filter out unwanted words
val finalList = filtered.limit(10) //take top 10 words
finalList.show()
