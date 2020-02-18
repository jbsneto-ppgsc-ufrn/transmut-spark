package example

import java.nio.charset.CodingErrorAction

import scala.io.Codec
import scala.io.Source
import scala.util.Try

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object MoviesRatingsAverage {

  // ratings.csv has the following data: userId,movieId,rating,timestamp
  // we want (movieId, (rating, 1.0))
  def parseRatings(r: String) = {
    if (r.split(",").length > 2 && Try(r.split(",")(1).toInt).isSuccess && Try(r.split(",")(2).toDouble).isSuccess) {
      val rating = r.split(",")
      Some((rating(1).toInt, (rating(2).toDouble, 1.0)))
    } else None
  }

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("./resources/movies.csv").getLines()
    for (line <- lines) {
      var movie = line.split(",")
      if (movie.length > 1 && Try(movie(0).toInt).isSuccess) {
        movieNames += (movie(0).toInt -> movie(1))
      }
    }

    return movieNames
  }

  def aggregateDoubleTuple(x: (Double, Double), y: (Double, Double)): (Double, Double) = (x._1 + y._1, x._2 + y._2)

  def mapDoubleTupleDivide(x: (Double, Double)) = x._1 / x._2

  def mapRatingsAverageToCSV(x: (Int, Double), movieNames: Broadcast[Map[Int, String]]) = x._1.toString + "," + movieNames.value(x._1).toString + "," + x._2.toString

  def moviesRatingsAverage(inputRDD: RDD[String], movieNames: Broadcast[Map[Int, String]]) = {

    val ratings: RDD[(Int, (Double, Double))] = inputRDD.flatMap(parseRatings)

    val ratingsSum: RDD[(Int, (Double, Double))] = ratings.reduceByKey(aggregateDoubleTuple)

    val ratingsAverage: RDD[(Int, Double)] = ratingsSum.mapValues(mapDoubleTupleDivide)

    val ratingsAverageSorted: RDD[(Int, Double)] = ratingsAverage.sortByKey(false)

    val resultsCVS: RDD[String] = ratingsAverageSorted.map((x: (Int, Double)) => mapRatingsAverageToCSV(x, movieNames))

    resultsCVS
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    var inputMLLatestRatingsURL = "hdfs://master:54310/user/hduser/ml-latest/ratings.csv" // default value
    var outputURL = "hdfs://master:54310/user/hduser/Output/movies-ratings-average" // default value

    if (args.length > 1) {
      inputMLLatestRatingsURL = args(0)
      outputURL = args(1)
    } else {
      println("Invalid arguments")
    }

    val conf = new SparkConf()
    conf.setAppName("MovieLens-Movies-Ratings-Average-With-Broadcast-Variable")
    val sparkContext = new SparkContext(conf)

    var movieNames = sparkContext.broadcast(loadMovieNames)

    val inputRDD = sparkContext.textFile(inputMLLatestRatingsURL)

    val resultsCVS = moviesRatingsAverage(inputRDD, movieNames)

    resultsCVS.saveAsTextFile(outputURL)
  }

}