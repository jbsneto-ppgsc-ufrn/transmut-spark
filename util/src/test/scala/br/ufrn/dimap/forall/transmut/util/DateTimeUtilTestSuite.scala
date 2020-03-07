package br.ufrn.dimap.forall.transmut.util

import org.scalatest.FunSuite
import java.time.LocalDateTime

class DateTimeUtilTestSuite extends FunSuite {

  test("Test Case 1 - DateTime From Datestamp") {
    val datestamp = "20200302143015"
    val dateTime = DateTimeUtil.getDateTimeFromDatestamp(datestamp)
    assert(dateTime.getYear == 2020)
    assert(dateTime.getMonth.getValue == 3)
    assert(dateTime.getDayOfMonth == 2)
    assert(dateTime.getHour == 14)
    assert(dateTime.getMinute == 30)
    assert(dateTime.getSecond == 15)
  }

  test("Test Case 2 - Datestamp From DateTime") {
    val dateTime = LocalDateTime.of(2020, 3, 2, 14, 30, 15)
    val datestamp = DateTimeUtil.getDatestampFromDateTime(dateTime)
    val expectedDatestamp = "20200302143015"
    assert(datestamp == expectedDatestamp)
  }

}