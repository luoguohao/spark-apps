package com.luogh.spark.common

/**
  * @author luogh
  */

sealed trait TaskParam

/**
  * 火车站/机场数据依赖参数: 1) 要查询的月份 2) 机场数据HDFS路径 3) 导入到vertica的表名
  */
case class TravelParam(monthId: Int, airDataPath: String, tableName: String) extends TaskParam

case class OtherDataParam(params: Array[String]) extends TaskParam

case class FetchTdidParam(monthId: Int) extends TaskParam

case class LoadVerticaParam(dataPath: String, tableName: String) extends TaskParam