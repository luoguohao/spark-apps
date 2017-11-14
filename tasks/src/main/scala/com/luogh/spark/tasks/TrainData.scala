package com.luogh.spark.tasks

import java.text.SimpleDateFormat
import java.util.Date

import com.luogh.spark.common.TravelSparkParam
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

/**
  * @author luogh 
  */
object TrainData {

  def run(args: TravelSparkParam)(sc: SparkContext): Unit = {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val inputPath = args.travelPath
    val tdidPath = args.tdidPath
    val outputPath = new Path(args.resultPath)


    val datas = sqlContext.read.load(s"${inputPath}/*.gz.parquet")
    datas.registerTempTable("train_tab")

    val tdid_scenery = sc.textFile(s"${tdidPath}")
      .filter(line => !line.isEmpty)
      .map { line => {
        val splits = line.split("\\|")
        TdidScenery(splits(0).trim, splits(1).trim.toInt)
      }
      }.toDF.registerTempTable("tdid_scenery_id")

    val format = new SimpleDateFormat("yyyy-MM-dd")
    val monthFormat = new SimpleDateFormat("yyyyMM")

    val resultRDD = sqlContext.sql("select tmp_tdid.tdid, tmp_tdid.sceneryId, tmp_air.sentTime, " +
      "tmp_air.id, tmp_air.poiid, tmp_air.name, tmp_air.address, tmp_air.lng, tmp_air.lat, " +
      "tmp_air.province, tmp_air.city, tmp_air.district  from tdid_scenery_id AS tmp_tdid INNER JOIN  " +
      "train_tab AS tmp_air ON tmp_tdid.tdid = tmp_air.tdid where length(tmp_air.poiid) > 0")
      .rdd
      .flatMap { r =>
        val tdid = r.getAs[String]("tdid")
        val scneneryId = r.getAs[Int]("sceneryId")
        val id = r.getAs[Int]("id")
        val poiid = r.getAs[String]("poiid")
        val name = r.getAs[String]("name")
        val address = r.getAs[String]("address")
        val lat = r.getAs[Double]("lat")
        val lng = r.getAs[Double]("lng")
        val province = r.getAs[String]("province")
        val city = r.getAs[String]("city")
        val district = r.getAs[String]("district")

        r.getAs[mutable.WrappedArray[Long]]("sentTime").map { x =>
          val date = new Date(x)
          (monthFormat.format(date).toInt, format.format(date))
        }.distinct.map { date =>
          (tdid, scneneryId, 1, date._1, id, poiid, name, address, lat, lng, province, city, district, date._2)
        }
      }.map(v => v.productIterator.mkString(",")).distinct()

    resultRDD.saveAsTextFile(outputPath.toString)
  }

  case class TdidScenery(tdid: String, sceneryId: Int)

}
