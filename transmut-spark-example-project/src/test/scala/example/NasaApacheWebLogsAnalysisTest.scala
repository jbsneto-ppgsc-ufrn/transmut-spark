package example

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class NasaApacheWebLogsAnalysisTest extends FunSuite with SharedSparkContext with RDDComparisons {

  test("test 1 - SHP - Same hosts with header") {

    val input1 = List("host	logname	time	method	url	response	bytes", "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")
    val input2 = List("host	logname	time	method	url	response	bytes", "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")

    val expected = List("199.72.81.55")

    val inputRDD1 = sc.parallelize(input1)
    val inputRDD2 = sc.parallelize(input2)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = NasaApacheWebLogsAnalysis.sameHostProblem(inputRDD1, inputRDD2)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }

  test("test 2 - UP - Same hosts with header") {

    val input1 = List("host	logname	time	method	url	response	bytes", "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")
    val input2 = List("host	logname	time	method	url	response	bytes", "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")

    val expected = List("199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")

    val inputRDD1 = sc.parallelize(input1)
    val inputRDD2 = sc.parallelize(input2)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = NasaApacheWebLogsAnalysis.unionLogsProblem(inputRDD1, inputRDD2)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }

  test("test 3 - SHP - Different hosts with header") {

    val input1 = List("host	logname	time	method	url	response	bytes", "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")
    val input2 = List("host	logname	time	method	url	response	bytes", "in24.inetnebr.com	-	807249601	GET	/shuttle/missions/sts-68/news/sts-68-mcc-05.txt	200	1839		")

    val expected: List[String] = List()

    val inputRDD1 = sc.parallelize(input1)
    val inputRDD2 = sc.parallelize(input2)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = NasaApacheWebLogsAnalysis.sameHostProblem(inputRDD1, inputRDD2)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }

  test("test 4 - UP - Different hosts with header") {

    val input1 = List("host	logname	time	method	url	response	bytes", "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")
    val input2 = List("host	logname	time	method	url	response	bytes", "in24.inetnebr.com	-	807249601	GET	/shuttle/missions/sts-68/news/sts-68-mcc-05.txt	200	1839		")

    val expected = List("199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	", "in24.inetnebr.com	-	807249601	GET	/shuttle/missions/sts-68/news/sts-68-mcc-05.txt	200	1839		")

    val inputRDD1 = sc.parallelize(input1)
    val inputRDD2 = sc.parallelize(input2)

    val expectedRDD = sc.parallelize(expected)

    val resultRDD = NasaApacheWebLogsAnalysis.unionLogsProblem(inputRDD1, inputRDD2)

    assert(None === compareRDD(resultRDD, expectedRDD))

  }

}