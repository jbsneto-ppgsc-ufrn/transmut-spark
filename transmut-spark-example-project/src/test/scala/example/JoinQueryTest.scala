package example

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class JoinQueryTest extends FunSuite with SharedSparkContext with RDDComparisons {

  test("test 1") {

    val inputRankings = List("url1,1,1")

    val inputUserVisits = List("0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1")

    val expected = List(("0.0.0.0", 1.0f, 1))

    val inputRankingsRDD = sc.parallelize(inputRankings)

    val inputUserVisitsRDD = sc.parallelize(inputUserVisits)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = JoinQuery.join(inputRankingsRDD, inputUserVisitsRDD)

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

  }

  test("test 2") {

    val inputRankings = List("url1,1,1")

    val inputUserVisits = List("0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1", "0.0.0.0,url1,1979-02-01,1.0,test,test,test,test,1")

    val expected = List(("0.0.0.0", 1.0f, 1))

    val inputRankingsRDD = sc.parallelize(inputRankings)

    val inputUserVisitsRDD = sc.parallelize(inputUserVisits)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = JoinQuery.join(inputRankingsRDD, inputUserVisitsRDD)

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

  }

  test("test 3") {

    val inputRankings = List("url1,1,1", "url2,2,2")

    val inputUserVisits = List("0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1")

    val expected = List(("0.0.0.0", 1.0f, 1))

    val inputRankingsRDD = sc.parallelize(inputRankings)

    val inputUserVisitsRDD = sc.parallelize(inputUserVisits)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = JoinQuery.join(inputRankingsRDD, inputUserVisitsRDD)

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

  }

  test("test 4") {

    val inputRankings = List("url1,1,1")

    val inputUserVisits = List("0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1", "0.0.0.1,url2,1980-02-01,2.0,test,test,test,test,2")

    val expected = List(("0.0.0.0", 1.0f, 1))

    val inputRankingsRDD = sc.parallelize(inputRankings)

    val inputUserVisitsRDD = sc.parallelize(inputUserVisits)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = JoinQuery.join(inputRankingsRDD, inputUserVisitsRDD)

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

  }

  test("test 5") {

    val inputRankings = List("url1,1,1", "url2,2,2")

    val inputUserVisits = List("0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1", "0.0.0.1,url2,1980-02-01,2.0,test,test,test,test,2")

    val expected = List(("0.0.0.1", 2.0f, 2), ("0.0.0.0", 1.0f, 1))

    val inputRankingsRDD = sc.parallelize(inputRankings)

    val inputUserVisitsRDD = sc.parallelize(inputUserVisits)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = JoinQuery.join(inputRankingsRDD, inputUserVisitsRDD)

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

  }

  test("test 6") {

    val inputRankings = List("url1,1,1", "url2,2,2", "url1,1,1")

    val inputUserVisits = List("0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1", "0.0.0.1,url2,1980-02-01,2.0,test,test,test,test,2", "0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1")

    val expected = List(("0.0.0.0", 4.0f, 1), ("0.0.0.1", 2.0f, 2))

    val inputRankingsRDD = sc.parallelize(inputRankings)

    val inputUserVisitsRDD = sc.parallelize(inputUserVisits)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = JoinQuery.join(inputRankingsRDD, inputUserVisitsRDD)

    assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

  }

}