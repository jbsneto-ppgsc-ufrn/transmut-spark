package example

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class ScanQueryTest extends FunSuite with SharedSparkContext with RDDComparisons {

  test("test 1 with one element RDD ") {

    val input = List("url1,301,1")

    val expected = List(("url1", 301))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = ScanQuery.scan(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }

  test("test 2 with false condition to filter ") {

    val input = List("url1,1,1")

    val expected: List[(String, Int)] = List()

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = ScanQuery.scan(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }

  test("test 3 with repeated elements RDD ") {

    val input = List("url1,301,1", "url1,301,1")

    val expected = List(("url1", 301), ("url1", 301))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = ScanQuery.scan(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }
}