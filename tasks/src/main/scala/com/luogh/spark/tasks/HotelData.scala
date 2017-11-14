package com.luogh.spark.tasks

import java.text.SimpleDateFormat
import java.util.Date

import com.luogh.spark.entity.Poi
import com.luogh.spark.tasks.AirportData.TdidScenery
import com.luogh.spark.util.HotelTool
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @author luogh 
  */
object HotelData {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName(s"${this.getClass.getName}")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    if (args.length != 3) {
      sys.error(s"expected three param <hotel_data_parquet_path> <tdid_data_gzip_path> <output_path> , but current are ${args.mkString(",")}");
      sys.exit(1);
    }

    val inputPath = args(0);
    val tdidPath = args(1);
    val outputPath = new Path(args(2))
    val fileSystem = FileSystem.get(sc.hadoopConfiguration);
    if (fileSystem.exists(outputPath)) {
      fileSystem.delete(outputPath, true);
    }

    sqlContext.read.load(s"${inputPath}/*.gz.parquet").registerTempTable("hotel_label")

    sc.textFile(s"${tdidPath}/*.gz")
      .filter(line => !line.isEmpty)
      .map { line => {
        val splits = line.split("\\|")
        TdidScenery(splits(0).trim, splits(1).trim.toInt)
      }
      }.toDF.registerTempTable("tdid_scenery_id")

    val format = new SimpleDateFormat("yyyy-MM-dd")

    // join hotel data with tdid
    val resultRDD = sqlContext.sql("select tmp_hotel.*, tmp_tdid.sceneryId from hotel_label AS tmp_hotel " +
      "INNER JOIN tdid_scenery_id AS tmp_tdid ON tmp_hotel.tdid = tmp_tdid.tdid")
      .registerTempTable("hotel_with_tdid")

    // poi data
    sqlContext.read.json("/datascience/user/yangyang.wang/wifi-gaode/hotel/201708/hotel_ap_all_gaode").registerTempTable("poi_data")

    // join poi data with hotel data
    sqlContext.sql("select tmp_hotel.*, tmp_poi.lng, tmp_poi.lat from hotel_with_tdid AS tmp_hotel inner join poi_data AS tmp_poi " +
      "ON tmp_hotel.poiid = tmp_poi.poiid")
      .rdd.flatMap { r =>
      val tdid = r.getAs[String]("tdid")
      val poiid = r.getAs[String]("poiid")
      val name = r.getAs[String]("name")
      val address = r.getAs[String]("address")
      val brand = r.getAs[String]("brand")
      val price = r.getAs[String]("price")
      val province = r.getAs[String]("province")
      val city = r.getAs[String]("city")
      val district = r.getAs[String]("district")
      val typeSmall = r.getAs[String]("type_small")
      val sceneryId = r.getAs[Int]("sceneryId")
      val lat = r.getAs[Double]("lat")
      val lng = r.getAs[Double]("lng")
      r.getAs[mutable.WrappedArray[Long]]("sentTime").map { x =>
        format.format(new Date(x))
      }.distinct.map { time =>
        val poi = new Poi(poiid, String.valueOf(lat), String.valueOf(lng), name, brand, price, typeSmall, province, city, district)
        val convert = HotelTool.normalBrand(poi);
        Hotel(tdid, time, sceneryId, poiid, lat, lng, name, convert.getBrand, price, convert.getType, province, city, district)
      }
    }.map(vo => vo.productIterator.mkString(",").replaceAll("null", "")).distinct()
      .saveAsTextFile(outputPath.toString)
  }


  case class Hotel(tdid: String, dateId: String, sceneryId: Int, poiId: String, poiLat: Double,
                   poiLng: Double, name: String, brand: String, price: String, typeName: String,
                   province: String, city: String, district: String)

}
