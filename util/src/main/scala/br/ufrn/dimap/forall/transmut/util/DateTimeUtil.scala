package br.ufrn.dimap.forall.transmut.util

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object DateTimeUtil {

  def defaultDatestampFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  
  def getCurrentDateTime = LocalDateTime.now()

  def getDateTimeFromDatestamp(datestamp: String): LocalDateTime = getDateTimeFromDatestamp(datestamp, defaultDatestampFormatter)

  def getDateTimeFromDatestamp(datestamp: String, formatter: DateTimeFormatter): LocalDateTime = {
    LocalDateTime.parse(datestamp, formatter)
  }

  def getDatestampFromDateTime(dateTime: LocalDateTime): String = getDatestampFromDateTime(dateTime, defaultDatestampFormatter)

  def getDatestampFromDateTime(dateTime: LocalDateTime, formatter: DateTimeFormatter): String = formatter.format(dateTime)

}