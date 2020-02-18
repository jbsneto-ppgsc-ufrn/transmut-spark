package example

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class WordCountTest extends FunSuite with SharedSparkContext with RDDComparisons {
  
  test("test 1 - Simple Test") {
    
    val input = List("A simple test")

    val expected: List[(String, Int)] = List(("A", 1), ("simple", 1), ("test", 1))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = WordCount.wordCount(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }
  
  test("test 2 - More Complex Test") {
    
    val input = List("A simple test", "A more complex test", "A more complex test")

    val expected: List[(String, Int)] = List(("A", 3), ("simple", 1), ("test", 3), ("more", 2), ("complex", 2))

    val inputRDD = sc.parallelize(input)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = WordCount.wordCount(inputRDD)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }
  
}