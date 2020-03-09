package com.cellariot.spark
​
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.broadcast.Broadcast


def loadMovieNames(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("/Users/ferdinand/Desktop/CEBD1261/exercise/ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }

def getElementsCount(word :String, dictionary:org.apache.spark.broadcast.Broadcast[Map[String,String]]):(String,Int) = {
dictionary.value.filter{ case (wording,wordType) => wording.equals((word))}.map(x => (x._2,1)).headOption.getOrElse(("unknown" -> 1))
}

def getElementsCount(word :String, dictionary:Map[String,String]):(String,Int) = {
dictionary.filter{ case (wording,wordType) => wording.equals((word))}.map(x => (x._2,1)).headOption.getOrElse(("unknown" -> 1)) //some dummy logic
}

Logger.getLogger("org").setLevel(Level.ERROR)
val sc = new SparkContext("local[*]", "WordCount")
var nameDict = sc.broadcast(loadMovieNames)
val lines = sc.textFile("/Users/ferdinand/Desktop/CEBD1261/exercise/ml-100k/u.data")
val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
val movieCounts = movies.reduceByKey( (x, y) => x + y )
val flipped = movieCounts.map( x => (x._2, x._1) )
val filteredMovies = flipped.filter(x => x._1 > 200) //rating larger than 200
val sortedMovies = filteredMovies.sortByKey(false) //sort in descending order to find top 10
val sortedMoviesWithNames = sortedMovies.map(x => (nameDict.value(x._2), x._1))
val sortedMoviesDF = spark.createDataFrame(sortedMoviesWithNames).toDF("Movie Title", "Ratings")
val top10 = sortedMoviesDF.limit(10)
top10.show()

