package com.luogh.spark.others

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSpec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author luogh 
  */
class BaseTestSpec extends FunSpec {

  case class Apple(id: Int)

  describe("测试==与===的区别") {
    it("测试 == ") {
      assert(Apple(1) == Apple(1))
    }

    it("测试 === ") {
      assert(Apple(1) !== Apple(1))
    }
  }

  describe("测试HashMap") {
    it("测试mutable.HashMap") {
      val m = new mutable.HashMap[Int, mutable.ArrayBuffer[String]]()
      val sets = m.getOrElseUpdate(1, new mutable.ArrayBuffer[String]())
      sets += "test1"

      val sets2 = m.getOrElseUpdate(1, new mutable.ArrayBuffer[String]())
      sets2 += "test2"

      assertResult(Map(1 -> ArrayBuffer("test1", "test2")))(m)
    }
  }

  it("测试case class是否实现Serilizable") {
    assert(Test("test").isInstanceOf[Serializable])
  }

  it("测试call-by-name") {
    def call(bean: => SparkContext): Unit = {
      println("===================>")
      println(bean.toString)
    }
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    call(new SparkContext(conf))
  }

}

case class Test(time: String)

class CallByName {
  println("init")
}
