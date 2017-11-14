package com.luogh.spark.common


/**
  * @author luogh 
  */
trait TaskContext extends AutoCloseable {

  type Param <: TaskParam

  def sysProperties: Map[String, String]

  def init(): this.type

  def params: Param
}

abstract class AbsTaskContext(val sysProperties: Map[String, String] = Map.empty) extends TaskContext {

  override def close(): Unit = {}

  override def init(): this.type = this
}

