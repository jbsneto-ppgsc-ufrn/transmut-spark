package example

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class MoviesRecommendationTest extends FunSuite with SharedSparkContext with RDDComparisons {

  test("test 1 - Similarities Table") {
    val input = List("1,1,1.0,964982703", "1,3,1.0,964981247")

    val expected = List(((1, 3), (1.0, 1)))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = MoviesRecommendation.moviesSimilaritiesTable(inputRDD)

    val result = resultRDD.collect()

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
  }

  test("test 2 - Recomendations") {
    val input = List("1,1,1.0,964982703", "1,3,1.0,964981247")

    val expected = List("1,3", "3,1")

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val moviesSimilaritiesTable = MoviesRecommendation.moviesSimilaritiesTable(inputRDD)

    val resultRDD = MoviesRecommendation.topNMoviesRecommendation(moviesSimilaritiesTable, 1, 0.9, 0)

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
  }

  test("test 3") {
    val input = List("2,3,1.0,964981247", "2,1,1.0,964982703", "1,3,1.0,964981247", "1,1,1.0,964982703")

    val expected = List(((1, 3), (0.9999999999999998, 2)))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = MoviesRecommendation.moviesSimilaritiesTable(inputRDD)

    val result = resultRDD.collect()

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
  }

  test("test 4") {
    val input = List("1,1,1.0,964982703", "1,3,1.0,964981247", "1,6,1.0,964982224", "2,1,0.0,964982703", "2,3,5.0,964981247", "2,6,5.0,964982224")

    val expected = List("3,6", "6,3")

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val moviesSimilaritiesTable = MoviesRecommendation.moviesSimilaritiesTable(inputRDD)

    val resultRDD = MoviesRecommendation.topNMoviesRecommendation(moviesSimilaritiesTable, 1, 0.9, 0)

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
  }

  test("test 5") {

    val input = List("2,6,5.0,964982224", "2,3,5.0,964981247", "2,1,0.0,964982703", "1,1,1.0,964982703", "1,3,1.0,964981247", "1,6,1.0,964982224")

    val expected = List(((1, 3), (0.19611613513818404, 2)), ((1, 6), (0.19611613513818404, 2)), ((3, 6), (1.0000000000000002, 2)))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = MoviesRecommendation.moviesSimilaritiesTable(inputRDD)

    val result = resultRDD.collect()

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
  }

}