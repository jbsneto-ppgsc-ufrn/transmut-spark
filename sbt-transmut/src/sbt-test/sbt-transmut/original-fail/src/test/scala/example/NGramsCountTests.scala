package example

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class NGramsCountTests extends FunSuite with SharedSparkContext with RDDComparisons {

  test("test 1 - One element with one sentence RDD ") {

    val input = List("This is a sentece.")

    val expected = List(
      (List("<start>", "<start>", "this"), 1),
      (List("<start>", "this", "is"), 1),
      (List("this", "is", "a"), 1),
      (List("is", "a", "sentece"), 1),
      (List("a", "sentece", "<end>"), 1))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD: RDD[(List[String], Int)] = NGramsCount.countNGrams(inputRDD)

    assert(None != compareRDD(resultRDD, expectedRDD))

  }

}