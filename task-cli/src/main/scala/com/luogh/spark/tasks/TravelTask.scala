package com.luogh.spark.tasks

import java.io._
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.luogh.spark.common._
import com.luogh.spark.util.{HDFSUtil, Util}
import org.apache.hadoop.fs.Path
import org.apache.livy.scalaapi.ScalaJobContext
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * @author luogh
  */
trait TravelTask extends Task {

  import TravelTask._
  import VerticaFunction._

  override type Context = TravelTaskContext
  val LOGGER = LoggerFactory.getLogger(TravelTask.getClass)

  override def buildTaskContext(args: Array[String]): Context = {
    assert(args.filterNot(_.isEmpty).length == 3, s"Travel Task expect 3 params, current ${args}")
    val path = args(1).trim
    assert(HDFSUtil.exist(path), s"InputPath ${path} doesn't exists.")
    val param = TravelParam(args(0).trim.toInt, path, args(2).trim)
    new TravelTaskContext(param)
  }

  override def execute(context: Context): Unit = {
    import com.luogh.spark.util.Util._

    val startTime = Stopwatch.createStarted()
    val params = context.params
    val month: MonthInfo = params.monthId
    val randKey = Util.uuid
    val tdidOutputFilePath = new Path(tdidDir, s"tdid_${month.monthId}_${randKey}")
    // 1. 从vertica中获取tdid
    withConnection { connect =>
      val writer = new PrintWriter(new OutputStreamWriter(HDFSUtil.fileSystem.create(tdidOutputFilePath)))
      try {
        val state = connect.prepareStatement(sql)
        state.setString(1, month.startDate)
        state.setString(2, month.endDate)
        val result = state.executeQuery()
        while (result.next()) {
          writer.println(s"${result.getString(2)}|${result.getInt(1)}") // write tdid|scnery_id
        }
        writer.flush()
      } finally {
        writer.close()
      }
    }

    require(HDFSUtil.exist(tdidOutputFilePath.toString), s"${tdidOutputFilePath} doesn't generate successful!")
    LOGGER.info(s">>>>>>>> load tdid from vertica into path ${tdidOutputFilePath} complete, " +
      s"total cost ${startTime.elapsed(TimeUnit.SECONDS)} s")

    // 2. 执行spark任务
    val outputSparkPath = s"${sparkResultPath}_${params.monthId}_${randKey}"
    if (HDFSUtil.exist(outputSparkPath)) {
      HDFSUtil.fileSystem.delete(new Path(outputSparkPath), true)
    }
    // first, upload relevant Jars
    context.uploadRelevantJarsForJobExecution
    // second, start execute
    val sparkJobHandler = context.scalaClient.submit(
      sparkJob(TravelSparkParam(tdidOutputFilePath.toString, params.airDataPath, outputSparkPath)))
    // finally, wait result
    Await.result(sparkJobHandler, Duration.Inf)

    val successFile = new Path(outputSparkPath, "_SUCCESS")
    require(HDFSUtil.exist(successFile.toString), s"${outputSparkPath} doesn't generate successful!")

    LOGGER.info(s">>>>>>>> spark job complete with result data ${outputSparkPath}, " +
      s"total cost ${startTime.elapsed(TimeUnit.SECONDS)} s")
    // 3. 将spark执行结果导入到vertica中

    withConnection { connect =>
      this.loadIntoVertica(LoadVerticaParam(outputSparkPath, params.tableName))(connect)
    }

    LOGGER.info(s">>>>>>>> load result into vertica complete, total " +
      s"cost ${startTime.stop().elapsed(TimeUnit.SECONDS)} s")
  }

  def sparkJob(param: TravelSparkParam): ScalaJobContext => Unit

}


object TravelTask {
  val sql =
    s"""
       |SELECT scenery_id, tdid FROM trip_tourist_visits_daily WHERE date_id >= ?
       |AND  date_id <= ? GROUP BY scenery_id, tdid
     """.stripMargin

  val tdidDir = "/fe/user/guohao.luo/talkingtrip/tdids/"

  if (!HDFSUtil.exist(tdidDir)) {
    HDFSUtil.fileSystem.mkdirs(new Path(tdidDir))
  }

  val sparkResultPath = "/user/hadoop/user/guohao.luo/talkingtrip/travel_task_result"
}

object TravelTaskContext {
  val LOGGER = LoggerFactory.getLogger(TravelTaskContext.getClass)
}


class TravelTaskContext(val params: TravelParam) extends AbsTaskContext with LivyTaskContext {

  type Param = TravelParam

  override val sparkAppName = "Travel_Task"
}






