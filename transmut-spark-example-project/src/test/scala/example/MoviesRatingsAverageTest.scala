package example

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class MoviesRatingsAverageTest extends FunSuite with SharedSparkContext with RDDComparisons {

  test("test 1 - One element") {

    val movieNames = sc.broadcast(MoviesRatingsAverage.loadMovieNames())

    val input = List("1,19,5.0,1978-10-17")

    val expected = List("19,Ace Ventura: When Nature Calls (1995),5.0")

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = MoviesRatingsAverage.moviesRatingsAverage(inputRDD, movieNames)

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

  }

  test("test 2 - Two elements same movie") {

    val movieNames = sc.broadcast(MoviesRatingsAverage.loadMovieNames())

    val input = List("1,19,5.0,1978-10-17", "1,19,3.0,1978-10-17")

    val expected = List("19,Ace Ventura: When Nature Calls (1995),4.0")

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = MoviesRatingsAverage.moviesRatingsAverage(inputRDD, movieNames)

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

  }

  test("test 3 - Three different movies") {

    val movieNames = sc.broadcast(MoviesRatingsAverage.loadMovieNames())

    val input = List("1,44,4.0,1978-10-17", "2,19,5.0,1978-10-17", "3,1721,5.0,1978-10-17")

    val expected = List("1721,Titanic (1997),5.0", "44,Mortal Kombat (1995),4.0", "19,Ace Ventura: When Nature Calls (1995),5.0")

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = MoviesRatingsAverage.moviesRatingsAverage(inputRDD, movieNames)

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

  }

  test("test 4 - Three elements same movie and two with same data") {

    val movieNames = sc.broadcast(MoviesRatingsAverage.loadMovieNames())

    val input = List("3,1721,5.0,1978-10-17", "3,1721,5.0,1978-10-17", "3,1721,2.0,1978-10-17")

    val expected = List("1721,Titanic (1997),4.0")

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = MoviesRatingsAverage.moviesRatingsAverage(inputRDD, movieNames)

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

  }
}