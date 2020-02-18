package example

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class NGramsCountTest extends FunSuite with SharedSparkContext with RDDComparisons {

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

    assert(None === compareRDD(resultRDD, expectedRDD))

  }

  test("test 2 - RDD with Empty String") {
    val input = List("")

    val expected: List[(List[String], Int)] = List()

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD: RDD[(List[String], Int)] = NGramsCount.countNGrams(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }

  test("test 3 - Two elements with the same sentence RDD") {
    val input = List("This is a sentece.", "This is a sentece.")

    val expected = List(
      (List("<start>", "<start>", "this"), 2),
      (List("<start>", "this", "is"), 2),
      (List("this", "is", "a"), 2),
      (List("is", "a", "sentece"), 2),
      (List("a", "sentece", "<end>"), 2))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD: RDD[(List[String], Int)] = NGramsCount.countNGrams(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }
  
  test("test 5 - More than two of the same sentence RDD") {
    
    val input = List("This is a sentece.", "This is a sentece.", "This is a sentece.")

    val expected = List(
      (List("<start>", "<start>", "this"), 3),
      (List("<start>", "this", "is"), 3),
      (List("this", "is", "a"), 3),
      (List("is", "a", "sentece"), 3),
      (List("a", "sentece", "<end>"), 3))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD: RDD[(List[String], Int)] = NGramsCount.countNGrams(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))
  }

  test("test 4 - One element with two of the same sentence RDD ") {
    val input = List("This is a sentece. This is a sentece.")

    val expected = List(
      (List("<start>", "<start>", "this"), 2),
      (List("<start>", "this", "is"), 2),
      (List("this", "is", "a"), 2),
      (List("is", "a", "sentece"), 2),
      (List("a", "sentece", "<end>"), 2))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD: RDD[(List[String], Int)] = NGramsCount.countNGrams(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }
  
  test("test 5 - Two elements with a sentence and the same sentence inverted RDD") {
    val input = List("This is a sentece.", "sentence a is This.")

    val expected = List(
      (List("<start>", "<start>", "this"), 1),
      (List("<start>", "<start>", "sentence"), 1),
      (List("<start>", "this", "is"), 1),
      (List("<start>", "sentence", "a"), 1),
      (List("this", "is", "a"), 1),
      (List("sentence", "a", "is"), 1),
      (List("is", "a", "sentece"), 1),
      (List("a", "is", "this"), 1),
      (List("a", "sentece", "<end>"), 1),
      (List("is", "this", "<end>"), 1))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD: RDD[(List[String], Int)] = NGramsCount.countNGrams(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }

}