package com.luogh.spark.others

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author luogh 
  */
object PoiData {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName(s"${this.getClass.getName}")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    sqlContext.read.parquet("/datascience/datacloud/datagroup/bigtask/liuxiaohong/tdid_travel_06/*.gz.parquet").registerTempTable("test")
    //    sqlContext.sql("select distinct tdid from test where poiid = 'B000A45467'").rdd.count()

    sc.textFile("/fe/user/guohao.luo/tdid_6_month").map { x =>
      val splits = x.split("\\|")
      (splits(0).trim, splits(1).trim)
    }.toDF("tdid", "scenery_id").registerTempTable("tmp_tdid")

    sqlContext.sql("select tmp_tdid.scenery_id, count(distinct tmp_tdid.tdid) as cnt from test inner join tmp_tdid on test.tdid " +
      "= tmp_tdid.tdid where test.poiid IN ('B000A45467', 'B000A84GDN', 'B000A7O1CU', 'B01C30003A') " +
      " and ((test.poiid = 'B000A45467' and tmp_tdid.scenery_id = 2) " +
      "or (test.poiid = 'B000A84GDN' and tmp_tdid.scenery_id = 1) " +
      "or (test.poiid = 'B000A7O1CU' and tmp_tdid.scenery_id = 4) " +
      "or (test.poiid = 'B01C30003A' and tmp_tdid.scenery_id = 10)" +
      ")" +
      "group by tmp_tdid.scenery_id").rdd.map { row =>
      val sceneryId = row.getAs[String]("scenery_id")
      val matchCnt = row.getAs[Long]("cnt")
      s"${sceneryId},${matchCnt}"
    }.saveAsTextFile("/fe/user/guohao.luo/tdid_cnt_result")

    sqlContext.sql("select poiid, count(distinct tdid) as cnt from test where poiid in ('B000A45467', 'B000A84GDN', 'B000A7O1CU', 'B01C30003A') group by poiid").rdd.map { row =>
      (row.getAs[String]("poiid"), row.getAs[Long]("cnt"))
    }.coalesce(1).collect().foreach(println _)

  }
}
