package example

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class AggregationQueryTest extends FunSuite with SharedSparkContext with RDDComparisons {

  test("test 1 with one element RDD ") {

    val input = List("0.0.0.0,test,1978-10-17,1.0,test,test,test,test,1")

    val expected = List(("0.0.0.0", 1.0f))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = AggregationQuery.aggregation(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }

  test("test 2 with repeated element RDD ") {

    val input = List("0.0.0.0,test,1978-10-17,1.0,test,test,test,test,1", "0.0.0.0,test,1978-10-17,1.0,test,test,test,test,1")

    val expected = List(("0.0.0.0", 2.0f))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = AggregationQuery.aggregation(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }

  test("test 3 with two elements with same key and different values RDD ") {

    val input = List("0.0.0.0,test,1978-10-17,1.0,test,test,test,test,1", "0.0.0.0,test,1978-10-17,2.0,test,test,test,test,1")

    val expected = List(("0.0.0.0", 3.0f))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = AggregationQuery.aggregation(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }
}