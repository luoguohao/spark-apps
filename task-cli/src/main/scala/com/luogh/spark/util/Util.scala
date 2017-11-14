package com.luogh.spark.util

import java.util.UUID

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.language.implicitConversions
/**
  * @author luogh 
  */
object Util {

  def main(args: Array[String]): Unit = print(uuid)

  implicit def convertMonthInfo(monthInfo: Int): MonthInfo = {
    val month = DateTime.parse(monthInfo.toString, DateTimeFormat.forPattern("yyyyMM"))
    val format = DateTimeFormat.forPattern("yyyy-MM-dd")
    MonthInfo(monthInfo, month.toString(format), month.dayOfMonth().withMaximumValue().toString(format))
  }

  def uuid: String = UUID.randomUUID().toString.replaceAll("-", "")

  case class MonthInfo(monthId: Int, startDate: String, endDate: String)

}
