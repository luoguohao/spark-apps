package com.luogh.spark

import com.luogh.spark.tasks.{AirportTask, LoadVerticaTask, TrainTask}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * @author luogh 
  */
object ScalaApp {

  val LOGGER = LoggerFactory.getLogger(ScalaApp.getClass)

  def main(args: Array[String]): Unit = {
    assert(args.length >= 1, "expected at least one params, current " + args.length)

    Try(args(0).toInt).flatMap { taskType =>
      val params = args.drop(1)
      taskType match {
        case 0 =>
          new AirportTask().start(params)
        case 1 =>
          new TrainTask().start(params)
        case 2 =>
          new LoadVerticaTask().start(params)
        case _ => sys.error(s"Invalid taskType. ${taskType}")
      }
    } match {
      case Success(_) =>
        LOGGER.info(s"Task ${args.mkString(",")} complete !")
      case Failure(exception) =>
        LOGGER.error(s"Task ${args.mkString(",")} failed!", exception)
        sys.exit(1)
    }
  }
}
