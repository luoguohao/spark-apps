package com.luogh.spark.common

import java.text.SimpleDateFormat

import scala.util.Try

/**
  * @author luogh 
  */
trait Task {

  import Task._

  type Context <: TaskContext

  def buildTaskContext(args: Array[String]): Context

  @throws[TaskExecuteException]
  def execute(context: Context): Unit

  def start(params: Array[String]): Try[_] = {
    val context = this.buildTaskContext(params)
    Try(withContext(context)(this.execute _))
  }
}

object Task {
  val monthFormat = new SimpleDateFormat("yyyyMM")

  def withContext[T <: TaskContext](context: => T)(func: T => Unit) = {
    try {
      context.init() // init context
      func(context)
    } finally {
      if (context != null) {
        context.close()
      }
    }
  }
}

case class TaskExecuteException(taskId: TaskId, message: String, e: Exception)
  extends RuntimeException(s"Task【${taskId}】 failed, Message【${message}】.", e)
